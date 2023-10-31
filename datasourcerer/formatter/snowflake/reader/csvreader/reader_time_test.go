package csvreader_test

import (
	"encoding/csv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/snowflake/reader/csvreader"
	stime "github.com/tsanton/dbt-unit-test-fusionizer/formatter/snowflake/reader/csvreader/time"
)

func Test_Time_ParseCsvHeaders(t *testing.T) {
	tests := []struct {
		name  string
		input []string
	}{
		{"Annotated", []string{"CreatedAt[time()]", "UpdatedAt[time(HH:mm:ssZ)]", "DeletedAt[time(HH:mm:ss,4)]"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			headers, err := csvreader.ParseCsvHeaders(nil, tt.input)
			assert.Nil(t, err)
			assert.Len(t, headers, 3)
			for _, header := range headers {
				_, ok := header.(*stime.Time)
				assert.True(t, ok)
			}
		})
	}
}

func Test_Time_Read_Annotated(t *testing.T) {
	t.Parallel()
	data := strings.TrimSpace(`
"CreatedAt[time()]","UpdatedAt[time(HH:mm:ssZ)]","DeletedAt[time(HH:mm:ss,4)]"
23:59:59,23:59:59Z,23:59:59
00:00:00,00:00:00Z,00:00:00
12:30:45,12:30:45Z,12:30:45
`)
	r := csv.NewReader(strings.NewReader(data))
	row, err := r.Read()
	assert.Nil(t, err)

	headers, err := csvreader.ParseCsvHeaders(nil, row)
	assert.Nil(t, err)

	content, err := csvreader.ParseCsvContent(nil, r, headers)
	assert.Nil(t, err)

	expected := strings.TrimSpace(`
SELECT '23:59:59'::TIME(9) AS CREATEDAT, '23:59:59'::TIME(9) AS UPDATEDAT, '23:59:59'::TIME(4) AS DELETEDAT
UNION ALL
SELECT '00:00:00'::TIME(9) AS CREATEDAT, '00:00:00'::TIME(9) AS UPDATEDAT, '00:00:00'::TIME(4) AS DELETEDAT
UNION ALL
SELECT '12:30:45'::TIME(9) AS CREATEDAT, '12:30:45'::TIME(9) AS UPDATEDAT, '12:30:45'::TIME(4) AS DELETEDAT
`)
	// fmt.Print(string(content))
	assert.Equal(t, expected, strings.TrimSpace(string(content)))
}
