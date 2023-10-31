package csvreader_test

import (
	"log/slog"
	"os"
	"testing"

	"github.com/tsanton/dbt-unit-test-fusionizer/formatter"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/snowflake/reader/csvreader"
)

var reader *csvreader.CsvlReader

func TestMain(m *testing.M) {
	var loggingLevel = new(slog.LevelVar)
	loggingLevel.Set(slog.LevelInfo)
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: loggingLevel}))
	slog.SetDefault(logger)

	reader = csvreader.NewCsvReader(logger, formatter.NewDefaultCsvConfig())

	exit := m.Run()

	os.Exit(exit)
}
