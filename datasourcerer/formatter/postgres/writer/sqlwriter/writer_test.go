package sqlwriter_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/postgres/writer/sqlwriter"
)

func Test_Postgres_Writer(t *testing.T) {
	writer := sqlwriter.NewSqlWriter(logger)
	buffer := &bytes.Buffer{}

	content := []byte("hello world!")
	err := writer.Write(buffer, content)
	if err != nil {
		t.Fatalf("Write method failed: %v", err)
	}

	assert.Equal(t, string(content)+"\n", buffer.String())
}
