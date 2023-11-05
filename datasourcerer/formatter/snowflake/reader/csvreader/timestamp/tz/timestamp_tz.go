package tz

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/tsanton/dbt-unit-test-fusionizer/formatter"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/snowflake/reader/csvreader/timestamp/utils"
)

var _ formatter.ICsvHeader = &TimestampTz{}

// Signature must contains "[boolean" (case insensitive) at any position and ends with ")]"
var timestampTimeZoneSignatureRegex = regexp.MustCompile(`(?i)^(\w+)\[timestamp_tz\((.*?)\)\]$`)

const (
	SnowflakeTimestampTimeZoneSignaturePrefix = "[timestamp_tz("
	defaultTimestampFormat                    = "2006-01-02 15:04:05"
	defaultPrecision                          = 9
)

type TimestampTz struct {
	fieldName          string
	format             string
	timestampSignature string
}

// GetName implements formatter.ICsvHeader
func (t *TimestampTz) GetName() string {
	return t.fieldName
}

// GetWriter implements formatter.ICsvHeader.
func (t *TimestampTz) GetWriter() func(value interface{}) ([]byte, error) {
	return func(value interface{}) ([]byte, error) {
		// Parse the timestamp based on the specified format
		timestamp, err := time.Parse(t.format, value.(string))
		if err != nil {
			return nil, fmt.Errorf("not able to convert value '%s' to timestamp using the '%s' format", value.(string), t.format)
		}

		// Convert the timestamp to a string in the default timestamp format
		timestampString := timestamp.Format(defaultTimestampFormat)

		// Return the formatted timestamp string with the appropriate Snowflake type and alias
		return []byte(fmt.Sprintf("'%s'::%s AS %s", timestampString, t.timestampSignature, t.fieldName)), nil
	}
}

// GetWriter implements formatter.ICsvHeader.
func (t *TimestampTz) ParseHeader(signature string) error {
	if !strings.HasSuffix(signature, "]") {
		return fmt.Errorf("invalid signature '%s'. Signature should be of the form <name>[timestamp_tz(<optional-format>,<optional-precision>)]", signature)
	}

	// Check for unbalanced parentheses
	if count := strings.Count(signature, "(") - strings.Count(signature, ")"); count != 0 {
		return fmt.Errorf("unbalanced parentheses in signature '%s'", signature)
	}

	// Extract the regex matches
	matches := timestampTimeZoneSignatureRegex.FindStringSubmatch(signature)

	if len(matches) != 3 {
		return fmt.Errorf("invalid signature '%s'. Expected () or (<optional-format>,<optional-precision>)", signature)
	}

	// Count arguments in parentheses
	args := strings.Count(matches[2], ",")
	if args > 1 {
		return fmt.Errorf("invalid signature '%s'. Expected () or (<optional-format>,<optional-precision>)", signature)
	}

	var err error
	var precision int
	params := strings.Split(matches[2], ",")

	// Parse optional format
	if len(params) > 0 && strings.TrimSpace(params[0]) != "" {
		if format, ok := utils.TimestampFormatMapper[strings.TrimSpace(params[0])]; ok {
			t.format = format
		} else {
			t.format = strings.TrimSpace(params[0])
		}
	} else {
		t.format = defaultTimestampFormat
	}

	// Parse optional precision
	if len(params) > 1 && strings.TrimSpace(params[1]) != "" {
		precision, err = strconv.Atoi(strings.TrimSpace(params[1]))
		if err != nil {
			return fmt.Errorf("invalid precision '%s' in signature '%s'. Expected int 0-9", strings.TrimSpace(params[1]), signature)
		}
		if precision < 0 || precision > 9 {
			return fmt.Errorf("precision must be between 0 and 9. Got '%d'", precision)
		}
	} else {
		precision = defaultPrecision
	}

	t.timestampSignature = fmt.Sprintf("TIMESTAMP_TZ(%d)", precision)

	t.fieldName = strings.ToUpper(strings.TrimSpace(matches[1]))

	return nil
}
