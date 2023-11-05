package sqlreader_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/postgres/reader/sqlreader"
)

func Test_Postgres_SqlReader(t *testing.T) {
	var buf bytes.Buffer
	msg := "Hello, World!"
	buf.WriteString(msg)

	reader := sqlreader.NewSqlReader(logger)

	content, err := reader.Read(&buf)

	assert.Nil(t, err)
	assert.Equal(t, []byte(msg), content)
}
