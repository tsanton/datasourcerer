package sqlwriter

import (
	"io"
	"log/slog"

	"github.com/tsanton/dbt-unit-test-fusionizer/formatter"
)

var _ formatter.IWriter = &SqlWriter{}

type SqlWriter struct {
	logger *slog.Logger
}

func NewSqlWriter(logger *slog.Logger) *SqlWriter {
	return &SqlWriter{
		logger: logger,
	}
}

// Write implements formatter.IWriter.
func (*SqlWriter) Write(w io.Writer, content []byte) error {
	_, err := w.Write(append(content, '\n'))
	return err
}
