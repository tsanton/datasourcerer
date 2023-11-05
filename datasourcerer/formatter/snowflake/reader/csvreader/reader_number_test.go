package csvreader_test

import (
	"encoding/csv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/snowflake/reader/csvreader"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/snowflake/reader/csvreader/number"
)

func Test_Number_ParseCsvHeaders(t *testing.T) {
	tests := []struct {
		name  string
		input []string
	}{
		{"Annotated", []string{"Age[number()]", "Height[number(14)]", "Weight[number(, 3)]"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			headers, err := csvreader.ParseCsvHeaders(reader, tt.input)
			assert.Nil(t, err)
			assert.Len(t, headers, 3)
			for _, header := range headers {
				_, ok := header.(*number.Number)
				assert.True(t, ok)
			}
		})
	}
}

func Test_Number_ReadCsv(t *testing.T) {
	t.Parallel()
	data := strings.TrimSpace(`
"Age[number()]","Height[number(4)]","Weight[number(, 3)]"
10,150.5,20.20
11,160.6,21.21
12,170.7,22.22
`)
	r := csv.NewReader(strings.NewReader(data))
	row, err := r.Read()
	assert.Nil(t, err)

	headers, err := csvreader.ParseCsvHeaders(reader, row)
	assert.Nil(t, err)

	content, err := csvreader.ParseCsvContent(reader, r, headers)
	assert.Nil(t, err)

	expected := strings.TrimSpace(`
SELECT 10::NUMBER(38,2) AS AGE, 150.5::NUMBER(4,2) AS HEIGHT, 20.20::NUMBER(38,3) AS WEIGHT
UNION ALL
SELECT 11::NUMBER(38,2) AS AGE, 160.6::NUMBER(4,2) AS HEIGHT, 21.21::NUMBER(38,3) AS WEIGHT
UNION ALL
SELECT 12::NUMBER(38,2) AS AGE, 170.7::NUMBER(4,2) AS HEIGHT, 22.22::NUMBER(38,3) AS WEIGHT
`)

	assert.Equal(t, expected, strings.TrimSpace(string(content)))

}
