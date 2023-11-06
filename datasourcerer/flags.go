package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/tsanton/dbt-unit-test-fusionizer/formatter"
	"gopkg.in/yaml.v3"
)

var _ flag.Value = &Cases{}

type Cases []string

// Set implements flag.Value.
func (s *Cases) Set(value string) error {
	*s = append(*s, value)
	return nil
}

// String implements flag.Value.
func (s *Cases) String() string {
	return strings.Join(*s, ", ")
}

type mainConfig struct {
	templateDir string
	unitTestDir string
	configPath  string
	workers     int
	crawlers    int
	parsers     int
	generators  int
	cases       Cases
	config      formatter.Config
}

func configureFlags() *mainConfig {
	cfg := &mainConfig{}
	flag.StringVar(&cfg.templateDir, "template-dir", "", "The directory to read template files from. Defaults to <DBT_PROJECT_DIR>/test-templates/")
	flag.StringVar(&cfg.unitTestDir, "test-dir", "", "The directory to output templated files. Defaults to <DBT_PROJECT_DIR>/tests/unit/")
	flag.StringVar(&cfg.configPath, "config-file", "", "The path to the datasourcerer config file. Defaults to <DBT_PROJECT_DIR>/.datasourcerer.yaml")
	flag.IntVar(&cfg.workers, "workers", 10, "How many workers to use. Will override the 'crawlers', 'parsers' and 'generators' flags. Defaults to 10")
	flag.IntVar(&cfg.crawlers, "crawlers", 10, "How many crawlers to run in parallell when parsing test templates, Defaults to 10")
	flag.IntVar(&cfg.parsers, "parsers", 10, "How many parsers to run in parallell when parsing and formatting data sources. Defaults to 10")
	flag.IntVar(&cfg.generators, "generators", 10, "How many generator to run in parallell when merging and outputting formatted data sources and test templates. Defaults to 10")
	flag.Var(&cfg.cases, "case", "file name to target for test case parsing")
	flag.Var(&cfg.cases, "c", "file name to target for test case parsing")
	return cfg
}

func (r *mainConfig) configureDefaults(logger *slog.Logger) error {
	var err error
	dbtProjectDir := os.Getenv("DBT_PROJECT_DIR")

	// Get the absolute path of the currently running binary
	executablePath, executablePathErr := os.Executable()
	if executablePathErr != nil {
		err = fmt.Errorf("unable to find executable path")
		logger.Error(err.Error())
		return err
	}

	//template-dir
	if r.templateDir == "" {
		// If the 'templateDir' is not provided, use the default.
		if dbtProjectDir != "" {
			r.templateDir = path.Join(dbtProjectDir, "test-templates")
		} else {
			r.templateDir = path.Join(executablePath, "test-templates")
		}
		logger.Debug(fmt.Sprintf("Using '%s' as the template directory location", r.templateDir))
	} else {
		// If 'templateDir' is provided, determine if it's absolute or relative.
		if filepath.IsAbs(r.templateDir) {
			logger.Debug(fmt.Sprintf("absolute 'template-dir' flag provided, looking for test templates in dir '%s'", r.templateDir))
		} else {
			// If 'dbtProjectDir' is set, use it as the base for the relative path.
			if dbtProjectDir != "" {
				r.templateDir = path.Join(dbtProjectDir, r.templateDir)
				logger.Debug(fmt.Sprintf("'dbtProjectDir' is set, using it as base for 'template-dir': '%s'", r.templateDir))
			} else {
				r.templateDir = path.Join(executablePath, r.templateDir)
				logger.Debug(fmt.Sprintf("relative 'template-dir' flag provided, using executable path as base: '%s'", r.templateDir))
			}
		}
	}
	// Ensure the 'templateDir' has a trailing slash
	r.templateDir = path.Clean(r.templateDir) + string(os.PathSeparator)

	logger.Info(fmt.Sprintf("looking for test templates in dir '%s'", r.templateDir))
	_, err = os.Stat(r.templateDir)
	if err != nil {
		err := fmt.Errorf("template directory '%s' does not exist", r.templateDir)
		logger.Error(err.Error())
	}

	//test-dir
	if r.unitTestDir == "" {
		// If 'unitTestDir' is not provided, determine the default directory.
		if dbtProjectDir != "" {
			r.unitTestDir = path.Join(dbtProjectDir, "tests/unit")
		} else {
			r.unitTestDir = path.Join(executablePath, "tests/unit")
		}
		logger.Debug(fmt.Sprintf("Using '%s' as the unit test directory location", r.unitTestDir))
	} else {
		// If 'unitTestDir' is provided, check if it's absolute or relative.
		if !filepath.IsAbs(r.unitTestDir) {
			// If it's relative, and 'dbtProjectDir' is set, use it as the base.
			if dbtProjectDir != "" {
				r.unitTestDir = path.Join(dbtProjectDir, r.unitTestDir)
				logger.Debug(fmt.Sprintf("Using 'dbtProjectDir' as base for relative 'test-dir': '%s'", r.unitTestDir))
			} else {
				// If 'dbtProjectDir' is not set, use the executable path as the base.
				r.unitTestDir = path.Join(executablePath, r.unitTestDir)
				logger.Debug(fmt.Sprintf("Using executable path as base for relative 'test-dir': '%s'", r.unitTestDir))
			}
		} else {
			logger.Debug(fmt.Sprintf("Absolute 'test-dir' flag provided, using as is: '%s'", r.unitTestDir))
		}
	}

	// Ensure the 'unitTestDir' has a trailing slash
	r.unitTestDir = path.Clean(r.unitTestDir) + string(os.PathSeparator)

	logger.Info(fmt.Sprintf("outputting generated unit tests to dir '%s'", r.unitTestDir))
	_, err = os.Stat(r.unitTestDir)
	if err != nil {
		err := fmt.Errorf("unit test directory '%s' does not exist", r.unitTestDir)
		logger.Error(err.Error())
	}

	//parallelism
	if r.workers != 10 {
		logger.Info("overriding 'crawlers', 'parsers' and 'generators' flags with 'workers' flag")
		r.crawlers = r.workers
		r.parsers = r.workers
		r.generators = r.workers
	}

	//config
	if r.configPath == "" {
		// If 'configPath' is not provided, determine the default directory.
		if dbtProjectDir != "" {
			r.configPath = path.Join(dbtProjectDir, ".datasourcerer.yaml")
		} else {
			r.configPath = path.Join(executablePath, ".datasourcerer.yaml")
		}
		logger.Debug(fmt.Sprintf("Using '%s' as the config file location", r.configPath))
	} else {
		// If 'configPath' is provided, check if it's absolute or relative.
		if !filepath.IsAbs(r.configPath) {
			// If it's relative, use the executable path as the base.
			r.configPath = path.Join(executablePath, r.configPath)
			logger.Debug(fmt.Sprintf("Relative 'config-file' flag provided. Looking for config file at '%s'", r.configPath))
		} else {
			// If it's absolute, use it as is.
			logger.Debug(fmt.Sprintf("Absolute 'config-file' flag provided. Using as is: '%s'", r.configPath))
		}
		// Ensure the 'configPath' is cleaned up
		r.configPath = path.Clean(r.configPath)
	}
	return err
}

func (r *mainConfig) parseConfigFile(logger *slog.Logger) error {
	_, err := os.Stat(r.configPath)
	if err != nil {
		logger.Error(fmt.Sprintf("config file '%s' does not exist", r.configPath))
		return err
	}
	yamlFile, err := os.ReadFile(r.configPath)
	if err != nil {
		logger.Error(fmt.Sprintf("error reading YAML file: %s\n", err))
		return err
	}

	err = yaml.Unmarshal(yamlFile, &r.config)
	if err != nil {
		logger.Error(fmt.Sprintf("Error parsing YAML file: %s", err))
		return err
	}

	return nil
}
