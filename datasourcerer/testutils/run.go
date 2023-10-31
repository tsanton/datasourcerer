package testutils

import (
	"log/slog"

	"github.com/tsanton/dbt-unit-test-fusionizer/datasourceparser"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/snowflake"
	"github.com/tsanton/dbt-unit-test-fusionizer/generator"
	"github.com/tsanton/dbt-unit-test-fusionizer/templatecrawler"
)

func Run(logger *slog.Logger, config *formatter.Config, rootDir string, outputDir string) {
	c := make(chan templatecrawler.DataSourceReference)
	dbtCrawler := templatecrawler.NewTestTemplateCrawler(logger, 1, rootDir)
	dataSourceParser := datasourceparser.NewDatasourceParser(
		logger,
		1,
		config,
		snowflake.Constructor(),
	)
	go dbtCrawler.Crawl(c)
	dataSourceParser.Parse(c)

	//Generate the dbt files with formatted inputted data
	tDefs := dbtCrawler.GetTestTemplates()
	dDefs := dataSourceParser.GetDataSources()
	generator := generator.NewTestGenerator(logger, 1, outputDir)
	_ = generator.Generate(tDefs, dDefs)
}
