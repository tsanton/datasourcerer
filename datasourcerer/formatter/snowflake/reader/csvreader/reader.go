package csvreader

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/tsanton/dbt-unit-test-fusionizer/formatter"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/snowflake/reader/csvreader/boolean"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/snowflake/reader/csvreader/date"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/snowflake/reader/csvreader/number"
	stime "github.com/tsanton/dbt-unit-test-fusionizer/formatter/snowflake/reader/csvreader/time"
	dtd "github.com/tsanton/dbt-unit-test-fusionizer/formatter/snowflake/reader/csvreader/timestamp/datetime"
	dtltz "github.com/tsanton/dbt-unit-test-fusionizer/formatter/snowflake/reader/csvreader/timestamp/ltz"
	dtntz "github.com/tsanton/dbt-unit-test-fusionizer/formatter/snowflake/reader/csvreader/timestamp/ntz"
	dttz "github.com/tsanton/dbt-unit-test-fusionizer/formatter/snowflake/reader/csvreader/timestamp/tz"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/snowflake/reader/csvreader/varchar"
)

var parserTypes = []struct {
	prefix string
	create func() formatter.ICsvHeader
}{
	// Add other parsers as needed
	{prefix: varchar.SnowflakeVarcharSignaturePrefix, create: func() formatter.ICsvHeader { return &varchar.Varchar{} }},
	{prefix: boolean.SnowflakeBooleanSignaturePrefix, create: func() formatter.ICsvHeader { return &boolean.Boolean{} }},
	{prefix: number.SnowflakeNumberSignaturePrefix, create: func() formatter.ICsvHeader { return &number.Number{} }},
	{prefix: date.SnowflakeDateSignaturePrefix, create: func() formatter.ICsvHeader { return &date.Date{} }},
	{prefix: stime.SnowflakeTimeSignaturePrefix, create: func() formatter.ICsvHeader { return &stime.Time{} }},
	{prefix: dtd.SnowflakeDatetimeSignaturePrefix, create: func() formatter.ICsvHeader { return &dtd.Datetime{} }},
	{prefix: dtntz.SnowflakeTimestampNoTimeZoneSignaturePrefix, create: func() formatter.ICsvHeader { return &dtntz.TimestampNtz{} }},
	{prefix: dtltz.SnowflakeTimestampLocalTimeZoneSignaturePrefix, create: func() formatter.ICsvHeader { return &dtltz.TimestampLtz{} }},
	{prefix: dttz.SnowflakeTimestampTimeZoneSignaturePrefix, create: func() formatter.ICsvHeader { return &dttz.TimestampTz{} }},
}

var _ formatter.IReader = &CsvlReader{}

type CsvlReader struct {
	logger *slog.Logger
	config formatter.CsvConfig
}

func NewCsvReader(logger *slog.Logger, config formatter.CsvConfig) *CsvlReader {
	return &CsvlReader{
		logger: logger,
		config: config,
	}
}

func (r *CsvlReader) Read(reader io.Reader) ([]byte, error) {
	var err error
	cr := csv.NewReader(reader)
	cr.Comma = []rune(r.config.Separator)[0]
	cr.Comment = []rune(r.config.Comment)[0]
	cr.FieldsPerRecord = -1 // Set to a positive number to enforce that many fields per record
	cr.LazyQuotes = false   // Allow lazy quotes
	cr.TrimLeadingSpace = r.config.TrimLeadingSpace
	cr.ReuseRecord = false // Reuse the record buffer

	raw, err := cr.Read()
	if err != nil {
		return nil, err
	}

	headers, err := r.parseCsvHeaders(raw)
	if err != nil {
		return nil, err
	}
	return r.parseCsvContent(cr, headers)

}

func (f *CsvlReader) parseCsvHeaders(headers []string) (map[int]formatter.ICsvHeader, error) {
	formatters := map[int]formatter.ICsvHeader{}
	for idx, header := range headers {
		col := strings.TrimSpace(strings.ToLower(header))
		if !strings.Contains(col, `[`) && !strings.HasSuffix(col, `)]`) {
			formatter := &varchar.Varchar{}
			if err := formatter.ParseHeader(header); err != nil {
				return nil, err
			}
			formatters[idx] = formatter
			continue
		}

		parsed := false
		for _, parserType := range parserTypes {
			if strings.Contains(col, parserType.prefix) && strings.HasSuffix(col, `)]`) {
				formatter := parserType.create()
				if err := formatter.ParseHeader(header); err != nil {
					return nil, err
				}
				formatters[idx] = formatter
				parsed = true
				break
			}
		}

		if !parsed {
			//TODO: Log error
			return nil, fmt.Errorf("unable to parse header `%s`", header)
		}
	}

	return formatters, nil
}

func (f *CsvlReader) parseCsvContent(r *csv.Reader, parsers map[int]formatter.ICsvHeader) ([]byte, error) {
	var buffer bytes.Buffer
	firstRecord := true
	for {
		record, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		if firstRecord {
			firstRecord = false
		} else {
			if _, err := buffer.WriteString("UNION ALL\n"); err != nil {
				return nil, err
			}
		}
		if _, err := buffer.WriteString("SELECT "); err != nil {
			return nil, err
		}

		for i, value := range record {
			parsedValue, err := parsers[i].GetWriter()(value)
			if err != nil {
				err := fmt.Errorf("error parsing value '%s' for column '%s' in line %d", value, parsers[i].GetName(), i+1)
				f.logger.Error(err.Error())
				buffer.Reset()
				buffer.WriteString(err.Error())
				return nil, err
			}
			if _, err := buffer.Write(parsedValue); err != nil {
				return nil, err
			}
			if _, err := buffer.WriteString(", "); err != nil {
				return nil, err
			}
		}
		if buffer.Len() > 0 {
			buffer.Truncate(buffer.Len() - 2) // remove the trailing comma
		}
		if err := buffer.WriteByte('\n'); err != nil {
			return nil, err
		}
	}
	return bytes.TrimRight(buffer.Bytes(), "\n"), nil
}
