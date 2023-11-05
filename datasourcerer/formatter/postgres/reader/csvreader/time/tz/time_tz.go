package tz

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/tsanton/dbt-unit-test-fusionizer/formatter"
)

var _ formatter.ICsvHeader = &TimeTz{}

// Signature must contain "[time" (case insensitive) at any position and ends with ")]"
var timeWithTimeZoneSignatureRegex = regexp.MustCompile(`(?i)^(\w+)\[time_tz\((.*?)\)\]$`)

const (
	PostgresTimeWithTimezoneSignaturePrefix = "[time_tz("
	defaultTimeFormat                       = "15:04:05"
	defaultPrecision                        = 6
)

var timeFormatMapper = map[string]string{
	"HH:mm:ss":         "15:04:05",              // Example: "14:30:45"
	"hh:mm:ss tt":      "03:04:05 PM",           // Example: "02:30:45 PM"
	"HH:mm":            "15:04",                 // Example: "14:30"
	"hh:mm tt":         "03:04 PM",              // Example: "02:30 PM"
	"HH:mm:ss.SSS":     "15:04:05.000",          // Example: "14:30:45.123"
	"hh:mm:ss.SSS tt":  "03:04:05.000 PM",       // Example: "02:30:45.123 PM"
	"HH:mm:ssZ":        "15:04:05Z07:00",        // Example: "14:30:45Z"
	"hh:mm:ss ttZ":     "03:04:05 PMZ07:00",     // Example: "02:30:45 PMZ"
	"HH:mm:ss.SSSZ":    "15:04:05.000Z07:00",    // Example: "14:30:45.123Z"
	"hh:mm:ss.SSS ttZ": "03:04:05.000 PMZ07:00", // Example: "02:30:45.123 PMZ"
}

type TimeTz struct {
	fieldName     string
	format        string
	timeSignature string
}

// GetName implements formatter.ICsvHeader
func (t *TimeTz) GetName() string {
	return t.fieldName
}

// GetWriter implements formatter.ICsvHeader.
func (t *TimeTz) GetWriter() func(value interface{}) ([]byte, error) {
	return func(value interface{}) ([]byte, error) {
		// Parse the time based on the specified format
		parsedTime, err := time.Parse(t.format, value.(string))
		if err != nil {
			return nil, fmt.Errorf("not able to convert value '%s' to time using the '%s' format", value.(string), t.format)
		}

		// Convert the time to a string in the default time format
		timeString := parsedTime.Format(defaultTimeFormat)

		// Return the formatted time string with the appropriate Snowflake type and alias
		return []byte(fmt.Sprintf("'%s'::%s as %s", timeString, t.timeSignature, t.fieldName)), nil
	}
}

// ParseHeader implements formatter.ICsvHeader.
func (t *TimeTz) ParseHeader(signature string) error {
	if !strings.HasSuffix(signature, "]") {
		return fmt.Errorf("invalid signature '%s'. Signature should be of the form <name>[time_tz(<optional-format>,<optional-precision>)]", signature)
	}

	// Check for unbalanced parentheses
	if count := strings.Count(signature, "(") - strings.Count(signature, ")"); count != 0 {
		return fmt.Errorf("unbalanced parentheses in signature '%s'", signature)
	}

	// Extract the regex matches
	matches := timeWithTimeZoneSignatureRegex.FindStringSubmatch(signature)

	if len(matches) != 3 {
		return fmt.Errorf("invalid signature '%s'. Expected () or (<optional-format>,<optional-precision>)", signature)
	}

	// Count arguments in parentheses
	args := strings.Count(matches[2], ",")
	if args > 1 {
		return fmt.Errorf("invalid signature '%s'. Expected () or (<optional-format>,<optional-precision>)", signature)
	}

	params := strings.Split(matches[2], ",")

	// Parse optional format
	if len(params) > 0 && strings.TrimSpace(params[0]) != "" {
		if format, ok := timeFormatMapper[strings.TrimSpace(params[0])]; ok {
			t.format = format
		} else {
			t.format = strings.TrimSpace(params[0])
		}
	} else {
		t.format = defaultTimeFormat
	}

	// Parse optional precision
	if len(params) > 1 && strings.TrimSpace(params[1]) != "" {
		precision, err := strconv.Atoi(strings.TrimSpace(params[1]))
		if err != nil {
			return fmt.Errorf("invalid precision '%s' in signature '%s'. Expected int 0-6", strings.TrimSpace(params[1]), signature)
		}
		if precision < 0 || precision > 6 {
			return fmt.Errorf("precision must be between 0 and 6. Got '%d'", precision)
		}
		t.timeSignature = fmt.Sprintf("time(%d) with time zone", precision)
	} else {
		t.timeSignature = fmt.Sprintf("time(%d) with time zone", defaultPrecision)
	}

	t.fieldName = strings.TrimSpace(matches[1])

	return nil
}
