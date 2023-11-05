package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/tsanton/dbt-unit-test-fusionizer/datasourceparser"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/postgres"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/snowflake"
	"github.com/tsanton/dbt-unit-test-fusionizer/generator"
	"github.com/tsanton/dbt-unit-test-fusionizer/templatecrawler"
)

var (
	logger *slog.Logger
	run    *mainConfig
)

func init() {
	/* Initialize logger */
	var loggingLevel = new(slog.LevelVar)
	loggingLevel.Set(slog.LevelInfo)

	// Check for LOG_LEVEL environment variable
	envLogLevel := strings.ToUpper(os.Getenv("LOG_LEVEL"))
	if envLogLevel != "" {
		switch envLogLevel {
		case "DEBUG":
			loggingLevel.Set(slog.LevelDebug)
		case "INFO":
			loggingLevel.Set(slog.LevelInfo)
		case "WARN":
			loggingLevel.Set(slog.LevelWarn)
		case "ERROR":
			loggingLevel.Set(slog.LevelError)
		default:
			panic("Invalid log level: " + envLogLevel)
		}
	}

	logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: loggingLevel}))
	slog.SetDefault(logger)

	/* Define flags */
	run = configureFlags()
}

func main() {
	/* Parse flags */
	flag.Parse()

	/* Set default values */
	err := run.configureDefaults(logger)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	/* Parse default config file */
	err = run.parseConfigFile(logger)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	c := make(chan templatecrawler.DataSourceReference)
	crawler := templatecrawler.NewTestTemplateCrawler(logger, run.crawlers, run.templateFileDir)

	var dataSources *map[string]datasourceparser.DataSourceFile
	switch run.config.Dialect {
	case "snowflake":
		parser := datasourceparser.NewDatasourceParser(
			logger,
			run.parsers,
			&run.config,
			snowflake.Constructor(),
		)
		go crawler.Crawl(c)
		parser.Parse(c)
		dataSources = parser.GetDataSources()
	case "postgres":
		parser := datasourceparser.NewDatasourceParser(
			logger,
			run.parsers,
			&run.config,
			postgres.Constructor(),
		)
		go crawler.Crawl(c)
		parser.Parse(c)
		dataSources = parser.GetDataSources()
	default:
		logger.Error(fmt.Sprintf("dialect type '%s' not supported.", run.config.Dialect))
		os.Exit(1)
	}

	//Generate the dbt files with formatted inputted data
	templates := crawler.GetTestTemplates()
	generator := generator.NewTestGenerator(logger, run.generators, run.testFileDir)
	_ = generator.Generate(templates, dataSources)
	logger.Info("Finished")
}
