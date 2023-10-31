package sqlreader

import (
	"io"
	"log/slog"

	"github.com/tsanton/dbt-unit-test-fusionizer/formatter"
)

var _ formatter.IReader = &SqlReader{}

type SqlReader struct {
	logger *slog.Logger
}

func NewSqlReader(logger *slog.Logger) *SqlReader {
	return &SqlReader{
		logger: logger,
	}
}

func (r *SqlReader) Read(reader io.Reader) ([]byte, error) {
	return io.ReadAll(reader)
}
