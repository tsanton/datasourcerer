package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
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
	templateFileDir string
	testFileDir     string
	configPath      string
	workers         int
	crawlers        int
	parsers         int
	generators      int
	cases           Cases
	config          formatter.Config
}

func configureFlags() *mainConfig {
	cfg := &mainConfig{}
	flag.StringVar(&cfg.templateFileDir, "file-dir", "", "The directory to read template files from. Defaults to <DBT_PROJECT_DIR>/test-templates")
	flag.StringVar(&cfg.templateFileDir, "fd", "", "The directory to read template files from. Defaults to <DBT_PROJECT_DIR>/test-templates")
	flag.StringVar(&cfg.testFileDir, "test-dir", "", "The directory to output templated files. Defaults to <DBT_PROJECT_DIR>/test/unit")
	flag.StringVar(&cfg.testFileDir, "td", "", "The directory to output templated files. Defaults to <DBT_PROJECT_DIR>/test/unit")
	flag.StringVar(&cfg.configPath, "config", "", "The path to the datasourcerer config file. Defaults to <DBT_PROJECT_DIR>/.datasourcerer.yaml")
	flag.StringVar(&cfg.configPath, "cfg", "", "The path to the datasourcerer config file. Defaults to <DBT_PROJECT_DIR>/.datasourcerer.yaml")
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

	//file-dir
	if r.templateFileDir == "" && dbtProjectDir == "" {
		logger.Error("environment variable 'DBT_PROJECT_DIR' not set and no file-dir flag provided")
	} else if r.templateFileDir == "" {
		logger.Debug("no file-dir flag provided, using default '<DBT_PROJECT_DIR>/test-templates'")
		r.templateFileDir = dbtProjectDir + "/test-templates"
	}
	_, err = os.Stat(r.templateFileDir)
	if err != nil {
		logger.Error("file-dir does not exist")
		return err
	}

	//test-dir
	if r.testFileDir == "" && dbtProjectDir == "" {
		logger.Error("environment variable 'DBT_PROJECT_DIR' not set and no test-dir flag provided")
	} else if r.testFileDir == "" {
		logger.Debug("no test-dir flag provided, using default '<DBT_PROJECT_DIR>/test/unit'")
		r.testFileDir = dbtProjectDir + "/test/unit"
	}
	_, err = os.Stat(r.testFileDir)
	if err != nil {
		logger.Error(fmt.Sprintf("file-dir '%s' does not exist", r.templateFileDir))
		return err
	}

	//parallelism
	if r.workers != 10 {
		logger.Info("overriding 'crawlers', 'parsers' and 'generators' flags with 'workers' flag")
		r.crawlers = r.workers
		r.parsers = r.workers
		r.generators = r.workers
	}

	//config
	if r.configPath == "" && dbtProjectDir == "" {
		logger.Error("environment variable 'DBT_PROJECT_DIR' not set and no config flag provided")
	} else if r.configPath == "" {
		logger.Debug("no config flag provided, using default '<DBT_PROJECT_DIR>/.datasourcerer.yaml'")
		r.configPath = dbtProjectDir + "/.datasourcerer.yaml"
	}

	return nil

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
