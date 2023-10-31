package datasourceparser

import (
	"io"
	"log/slog"

	"github.com/tsanton/dbt-unit-test-fusionizer/formatter"
)

var _ formatter.IDataSourceFormatter = &ErrorFormatter{}

type ErrorFormatter struct {
	logger  *slog.Logger
	reader  formatter.IReader
	content []byte
	writer  formatter.IWriter
}

func NewErrorFormatter(logger *slog.Logger, err error) *ErrorFormatter {
	return &ErrorFormatter{
		logger:  logger,
		reader:  nil,
		writer:  nil,
		content: []byte(err.Error()),
	}
}

// Read implements formatter.IDataSourceFormatter.
func (s *ErrorFormatter) Read(r io.Reader) error {
	return nil
}

// Write implements formatter.IDataSourceFormatter.
func (s *ErrorFormatter) Write(writer io.Writer) error {
	_, err := writer.Write(append(s.content, '\n'))
	return err
}
