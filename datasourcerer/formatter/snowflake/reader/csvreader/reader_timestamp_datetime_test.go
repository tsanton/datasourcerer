package csvreader_test

import (
	"encoding/csv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/snowflake/reader/csvreader"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/snowflake/reader/csvreader/timestamp/datetime"
)

func Test_Timestamp_Datetime_ParseCsvHeaders(t *testing.T) {
	tests := []struct {
		name  string
		input []string
	}{
		{"Annotated", []string{"CreatedAt[datetime()]", "UpdatedAt[datetime(yyyy-MM-ddTHH:mm:ssZ)]", "DeletedAt[datetime(MM/dd/yyyy HH:mm:ss,3)]"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			headers, err := csvreader.ParseCsvHeaders(reader, tt.input)
			assert.Nil(t, err)
			assert.Len(t, headers, 3)
			for _, header := range headers {
				_, ok := header.(*datetime.Datetime)
				assert.True(t, ok)
			}
		})
	}
}

func Test_Timestamp_ReadCsv(t *testing.T) {
	t.Parallel()
	data := strings.TrimSpace(`
"CreatedAt[datetime()]","UpdatedAt[datetime(yyyy-MM-ddTHH:mm:ssZ,4)]","DeletedAt[datetime(MM/dd/yyyy HH:mm:ss,3)]"
2000-12-31 23:59:59,2000-12-31T23:59:59Z,12/31/2000 23:59:59
1990-01-01 00:00:00,1990-01-01T00:00:00Z,01/01/1990 00:00:00
1980-06-15 12:30:45,1980-06-15T12:30:45Z,06/15/1980 12:30:45
`)
	r := csv.NewReader(strings.NewReader(data))
	row, err := r.Read()
	assert.Nil(t, err)

	headers, err := csvreader.ParseCsvHeaders(reader, row)
	assert.Nil(t, err)

	content, err := csvreader.ParseCsvContent(reader, r, headers)
	assert.Nil(t, err)

	expected := strings.TrimSpace(`
SELECT '2000-12-31 23:59:59'::DATETIME(9) AS CREATEDAT, '2000-12-31 23:59:59'::DATETIME(4) AS UPDATEDAT, '2000-12-31 23:59:59'::DATETIME(3) AS DELETEDAT
UNION ALL
SELECT '1990-01-01 00:00:00'::DATETIME(9) AS CREATEDAT, '1990-01-01 00:00:00'::DATETIME(4) AS UPDATEDAT, '1990-01-01 00:00:00'::DATETIME(3) AS DELETEDAT
UNION ALL
SELECT '1980-06-15 12:30:45'::DATETIME(9) AS CREATEDAT, '1980-06-15 12:30:45'::DATETIME(4) AS UPDATEDAT, '1980-06-15 12:30:45'::DATETIME(3) AS DELETEDAT
`)
	assert.Equal(t, expected, strings.TrimSpace(string(content)))
}
