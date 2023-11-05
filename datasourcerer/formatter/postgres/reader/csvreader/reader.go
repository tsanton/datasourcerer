package csvreader

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/tsanton/dbt-unit-test-fusionizer/formatter"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/postgres/reader/csvreader/bigint"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/postgres/reader/csvreader/boolean"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/postgres/reader/csvreader/date"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/postgres/reader/csvreader/integer"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/postgres/reader/csvreader/jsonb"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/postgres/reader/csvreader/numeric"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/postgres/reader/csvreader/smallint"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/postgres/reader/csvreader/text"
	timentz "github.com/tsanton/dbt-unit-test-fusionizer/formatter/postgres/reader/csvreader/time/ntz"
	timetz "github.com/tsanton/dbt-unit-test-fusionizer/formatter/postgres/reader/csvreader/time/tz"
	timestampntz "github.com/tsanton/dbt-unit-test-fusionizer/formatter/postgres/reader/csvreader/timestamp/ntz"
	timestamptz "github.com/tsanton/dbt-unit-test-fusionizer/formatter/postgres/reader/csvreader/timestamp/tz"
)

var parserTypes = []struct {
	prefix string
	create func() formatter.ICsvHeader
}{
	// Add other parsers as needed
	{prefix: text.PostgresTextSignaturePrefix, create: func() formatter.ICsvHeader { return &text.Text{} }},
	{prefix: smallint.PostgresSmallintSignaturePrefix, create: func() formatter.ICsvHeader { return &smallint.SmallInt{} }},
	{prefix: integer.PostgresIntegerSignaturePrefix, create: func() formatter.ICsvHeader { return &integer.Integer{} }},
	{prefix: bigint.PostgresBigintSignaturePrefix, create: func() formatter.ICsvHeader { return &bigint.BigInt{} }},
	{prefix: numeric.PostgresNumericSignaturePrefix, create: func() formatter.ICsvHeader { return &numeric.Numeric{} }},
	{prefix: boolean.PostgresBooleanSignaturePrefix, create: func() formatter.ICsvHeader { return &boolean.Boolean{} }},
	{prefix: jsonb.PostgresJsonbSignaturePrefix, create: func() formatter.ICsvHeader { return &jsonb.Jsonb{} }},
	{prefix: date.PostgresDateSignaturePrefix, create: func() formatter.ICsvHeader { return &date.Date{} }},
	{prefix: timentz.PostgresTimeWithoutTimezoneSignaturePrefix, create: func() formatter.ICsvHeader { return &timentz.TimeNtz{} }},
	{prefix: timetz.PostgresTimeWithTimezoneSignaturePrefix, create: func() formatter.ICsvHeader { return &timetz.TimeTz{} }},
	{prefix: timestampntz.PostgresTimestampNoTimeZoneSignaturePrefix, create: func() formatter.ICsvHeader { return &timestampntz.TimestampNtz{} }},
	{prefix: timestamptz.PostgresTimestampWithTimeZoneSignaturePrefix, create: func() formatter.ICsvHeader { return &timestamptz.TimestampTz{} }},
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
			formatter := &text.Text{}
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
