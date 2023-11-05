package csvreader_test

import (
	"encoding/csv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/snowflake/reader/csvreader"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/snowflake/reader/csvreader/date"
)

func Test_Date_ParseCsvHeaders(t *testing.T) {
	tests := []struct {
		name  string
		input []string
	}{
		{"Annotated", []string{"DateOfBirth[date()]", "WeddingDate[date(MM/dd/yyyy)]", "DivorseDate[date(dd MMM yyyy)]"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			headers, err := csvreader.ParseCsvHeaders(reader, tt.input)
			assert.Nil(t, err)
			assert.Len(t, headers, 3)
			for _, header := range headers {
				_, ok := header.(*date.Date)
				assert.True(t, ok)
			}
		})
	}
}

func Test_Date_ReadCsv(t *testing.T) {
	t.Parallel()
	data := strings.TrimSpace(`
"DateOfBirth[date()]","WeddingDate[date(MM/dd/yyyy)]","DivorseDate[date(dd MMM yyyy)]"
1990-01-15,06/20/2010,15 Apr 2015
1985-12-25,12/31/2000,01 Jan 2010
1975-07-07,07/04/1995,04 Aug 2005

`)
	r := csv.NewReader(strings.NewReader(data))
	row, err := r.Read()
	assert.Nil(t, err)

	headers, err := csvreader.ParseCsvHeaders(reader, row)
	assert.Nil(t, err)

	content, err := csvreader.ParseCsvContent(reader, r, headers)
	assert.Nil(t, err)

	expected := strings.TrimSpace(`
SELECT '1990-01-15'::DATE AS DATEOFBIRTH, '2010-06-20'::DATE AS WEDDINGDATE, '2015-04-15'::DATE AS DIVORSEDATE
UNION ALL
SELECT '1985-12-25'::DATE AS DATEOFBIRTH, '2000-12-31'::DATE AS WEDDINGDATE, '2010-01-01'::DATE AS DIVORSEDATE
UNION ALL
SELECT '1975-07-07'::DATE AS DATEOFBIRTH, '1995-07-04'::DATE AS WEDDINGDATE, '2005-08-04'::DATE AS DIVORSEDATE
`)
	// fmt.Print(string(content))
	assert.Equal(t, expected, strings.TrimSpace(string(content)))

}
