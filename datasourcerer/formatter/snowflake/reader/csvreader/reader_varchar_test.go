package csvreader_test

import (
	"encoding/csv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/snowflake/reader/csvreader"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/snowflake/reader/csvreader/varchar"
)

func Test_Varchar_ParseCsvHeaders(t *testing.T) {
	tests := []struct {
		name  string
		input []string
	}{
		{"Vanilla", []string{"Name", "Address", "City"}},
		{"Annotated", []string{"Name", "Address[varchar()]", "City[varchar(40)]"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			headers, err := csvreader.ParseCsvHeaders(reader, tt.input)
			assert.Nil(t, err)
			assert.Len(t, headers, 3)
			for _, header := range headers {
				_, ok := header.(*varchar.Varchar)
				assert.True(t, ok)
			}
		})
	}
}

func Test_Varchar_ReadCsv(t *testing.T) {
	t.Parallel()
	data := strings.TrimSpace(`
Name,Address[varchar()],City[varchar(50)]
John,123 Main St,New York
Jane,456 Main St,New York
Kane,901 Main St,New York
`)
	r := csv.NewReader(strings.NewReader(data))
	row, err := r.Read()
	assert.Nil(t, err)

	headers, err := csvreader.ParseCsvHeaders(reader, row)
	assert.Nil(t, err)

	content, err := csvreader.ParseCsvContent(reader, r, headers)
	assert.Nil(t, err)

	expected := strings.TrimSpace(`
SELECT 'John'::VARCHAR(16777216) AS NAME, '123 Main St'::VARCHAR(16777216) AS ADDRESS, 'New York'::VARCHAR(50) AS CITY
UNION ALL
SELECT 'Jane'::VARCHAR(16777216) AS NAME, '456 Main St'::VARCHAR(16777216) AS ADDRESS, 'New York'::VARCHAR(50) AS CITY
UNION ALL
SELECT 'Kane'::VARCHAR(16777216) AS NAME, '901 Main St'::VARCHAR(16777216) AS ADDRESS, 'New York'::VARCHAR(50) AS CITY
`)
	assert.Equal(t, expected, strings.TrimSpace(string(content)))
}
