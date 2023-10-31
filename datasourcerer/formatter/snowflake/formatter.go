package snowflake

import (
	"fmt"
	"io"
	"log/slog"

	"github.com/tsanton/dbt-unit-test-fusionizer/formatter"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/snowflake/reader/csvreader"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/snowflake/reader/sqlreader"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/snowflake/writer/sqlwriter"
)

var _ formatter.IDataSourceFormatter = &SnowflakeFormatter{}

type SnowflakeFormatter struct {
	logger  *slog.Logger
	reader  formatter.IReader
	content []byte
	writer  formatter.IWriter
}

func Constructor() func(*slog.Logger, *formatter.Config) *SnowflakeFormatter {
	return func(logger *slog.Logger, config *formatter.Config) *SnowflakeFormatter {
		var reader formatter.IReader
		switch config.Filetype {
		case formatter.ParserInputTypeSql:
			reader = sqlreader.NewSqlReader(logger)
		case formatter.ParserInputTypeCsv:
			reader = csvreader.NewCsvReader(logger, config.CSV)
		default:
			panic(fmt.Sprintf("invalid input type: '%s'", config.Filetype))
		}

		return &SnowflakeFormatter{
			logger: logger,
			reader: reader,
			writer: sqlwriter.NewSqlWriter(logger),
		}
	}
}

// Read implements formatter.IDataSourceFormatter.
func (s *SnowflakeFormatter) Read(r io.Reader) error {
	var err error
	s.content, err = s.reader.Read(r)
	return err
}

// Write implements formatter.IDataSourceFormatter.
func (s *SnowflakeFormatter) Write(writer io.Writer) error {
	return s.writer.Write(writer, s.content)
}
