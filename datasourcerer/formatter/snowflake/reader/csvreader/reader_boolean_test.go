package csvreader_test

import (
	"encoding/csv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/snowflake/reader/csvreader"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/snowflake/reader/csvreader/boolean"
)

func Test_Boolean_ParseCsvHeaders(t *testing.T) {
	tests := []struct {
		name  string
		input []string
	}{
		{"Annotated", []string{"Handsome[boolean()]", "Ugly[boolean(T)]", "Weird[boolean(TRUE, FALSE)]"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			headers, err := csvreader.ParseCsvHeaders(reader, tt.input)
			assert.Nil(t, err)
			assert.Len(t, headers, 3)
			for _, header := range headers {
				_, ok := header.(*boolean.Boolean)
				assert.True(t, ok)
			}
		})
	}
}

func Test_Boolean_ReadCsv(t *testing.T) {
	t.Parallel()
	data := strings.TrimSpace(`
"Handsome[boolean()]","Ugly[boolean(T)]","Weird[boolean(TRUE,FALSE)]"
true,false,FALSE
false,T,TRUE
true,false,FALSE
`)
	r := csv.NewReader(strings.NewReader(data))
	row, err := r.Read()
	assert.Nil(t, err)

	headers, err := csvreader.ParseCsvHeaders(reader, row)
	assert.Nil(t, err)

	content, err := csvreader.ParseCsvContent(reader, r, headers)
	assert.Nil(t, err)

	expected := strings.TrimSpace(`
SELECT true::BOOLEAN AS HANDSOME, false::BOOLEAN AS UGLY, false::BOOLEAN AS WEIRD
UNION ALL
SELECT false::BOOLEAN AS HANDSOME, true::BOOLEAN AS UGLY, true::BOOLEAN AS WEIRD
UNION ALL
SELECT true::BOOLEAN AS HANDSOME, false::BOOLEAN AS UGLY, false::BOOLEAN AS WEIRD
`)

	assert.Equal(t, expected, strings.TrimSpace(string(content)))

}
