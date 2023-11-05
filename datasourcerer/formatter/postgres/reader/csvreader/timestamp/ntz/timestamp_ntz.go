package ntz

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/tsanton/dbt-unit-test-fusionizer/formatter"
)

var _ formatter.ICsvHeader = &TimestampNtz{}

// Signature must contains "[boolean" (case insensitive) at any position and ends with ")]"
var timestampNoTimeZoneSignatureRegex = regexp.MustCompile(`(?i)^(\w+)\[timestamp\((.*?)\)\]$`)

const (
	PostgresTimestampNoTimeZoneSignaturePrefix = "[timestamp("
	defaultTimestampFormat                     = "2006-01-02 15:04:05"
	defaultPrecision                           = 6
)

var timestampFormatMapper = map[string]string{
	"yyyy-MM-dd HH:mm:ss":      "2006-01-02 15:04:05",      // Example: "2023-10-24 14:30:45"
	"yyyy-MM-ddThh:mm:ssZ":     "2006-01-02T03:04:05Z",     // Example: "2023-10-24T02:30:45Z"
	"yyyy-MM-ddTHH:mm:ssZ":     "2006-01-02T15:04:05Z",     // Example: "2023-10-24T14:30:45Z"
	"yyyy-MM-dd HH:mm:ss.SSSZ": "2006-01-02 15:04:05.000Z", // Example: "2023-10-24 14:30:45.123Z"
	"yyyy-MM-ddTHH:mm:ss.SSSZ": "2006-01-02T15:04:05.000Z", // Example: "2023-10-24T14:30:45.123Z"
	"yyyy-MM-dd HH:mm:ss.SSS":  "2006-01-02 15:04:05.000",  // Example: "2023-10-24 14:30:45.123"
	"yyyy-MM-ddThh:mm:ss":      "2006-01-02T03:04:05",      // Example: "2023-10-24T02:30:45"
	"yyyy-MM-ddTHH:mm:ss":      "2006-01-02T15:04:05",      // Example: "2023-10-24T14:30:45"
	"yyyy/MM/dd HH:mm:ss":      "2006/01/02 15:04:05",      // Example: "2023/10/24 14:30:45"
	"yyyy/MM/dd HH:mm:ss.SSSZ": "2006/01/02 15:04:05.000Z", // Example: "2023/10/24 14:30:45.123Z"
	"yyyy/MM/ddTHH:mm:ss.SSSZ": "2006/01/02T15:04:05.000Z", // Example: "2023/10/24T14:30:45.123Z"
	"yyyy/MM/dd HH:mm:ss.SSS":  "2006/01/02 15:04:05.000",  // Example: "2023/10/24 14:30:45.123"
	"yyyy/MM/ddThh:mm:ss":      "2006/01/02T03:04:05",      // Example: "2023/10/24T02:30:45"
	"yyyy/MM/ddTHH:mm:ss":      "2006/01/02T15:04:05",      // Example: "2023/10/24T14:30:45"
	"MM-dd-yyyy HH:mm:ss":      "01-02-2006 15:04:05",      // Example: "10-24-2023 14:30:45"
	"MM-dd-yyyy HH:mm:ss.SSSZ": "01-02-2006 15:04:05.000Z", // Example: "10-24-2023 14:30:45.123Z"
	"MM-dd-yyyyTHH:mm:ss.SSSZ": "01-02-2006T15:04:05.000Z", // Example: "10-24-2023T14:30:45.123Z"
	"MM-dd-yyyy HH:mm:ss.SSS":  "01-02-2006 15:04:05.000",  // Example: "10-24-2023 14:30:45.123"
	"MM-dd-yyyyThh:mm:ss":      "01-02-2006T03:04:05",      // Example: "10-24-2023T02:30:45"
	"MM-dd-yyyyTHH:mm:ss":      "01-02-2006T15:04:05",      // Example: "10-24-2023T14:30:45"
	"MM/dd/yyyy HH:mm:ss":      "01/02/2006 15:04:05",      // Example: "10/24/2023 14:30:45"
	"MM/dd/yyyy HH:mm:ss.SSSZ": "01/02/2006 15:04:05.000Z", // Example: "10/24/2023 14:30:45.123Z"
	"MM/dd/yyyyTHH:mm:ss.SSSZ": "01/02/2006T15:04:05.000Z", // Example: "10/24/2023T14:30:45.123Z"
	"MM/dd/yyyy HH:mm:ss.SSS":  "01/02/2006 15:04:05.000",  // Example: "10/24/2023 14:30:45.123"
	"MM/dd/yyyyThh:mm:ss":      "01/02/2006T03:04:05",      // Example: "10/24/2023T02:30:45"
	"MM/dd/yyyyTHH:mm:ss":      "01/02/2006T15:04:05",      // Example: "10/24/2023T14:30:45"
}

type TimestampNtz struct {
	fieldName          string
	format             string
	timestampSignature string
}

// GetName implements formatter.ICsvHeader
func (t *TimestampNtz) GetName() string {
	return t.fieldName
}

// GetWriter implements formatter.ICsvHeader.
func (t *TimestampNtz) GetWriter() func(value interface{}) ([]byte, error) {
	return func(value interface{}) ([]byte, error) {
		// Parse the timestamp based on the specified format
		timestamp, err := time.Parse(t.format, value.(string))
		if err != nil {
			return nil, fmt.Errorf("not able to convert value '%s' to timestamp using the '%s' format", value.(string), t.format)
		}

		// Convert the timestamp to a string in the default timestamp format
		timestampString := timestamp.Format(defaultTimestampFormat)

		// Return the formatted timestamp string with the appropriate Snowflake type and alias
		return []byte(fmt.Sprintf("'%s'::%s as %s", timestampString, t.timestampSignature, t.fieldName)), nil
	}
}

// TODO: refactor
// GetWriter implements formatter.ICsvHeader.
func (t *TimestampNtz) ParseHeader(signature string) error {
	if !strings.HasSuffix(signature, "]") {
		return fmt.Errorf("invalid signature '%s'. Signature should be of the form <name>[timestamp(<optional-format>,<optional-precision>)]", signature)
	}

	// Check for unbalanced parentheses
	if count := strings.Count(signature, "(") - strings.Count(signature, ")"); count != 0 {
		return fmt.Errorf("unbalanced parentheses in signature '%s'", signature)
	}

	// Extract the regex matches
	matches := timestampNoTimeZoneSignatureRegex.FindStringSubmatch(signature)

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
		if format, ok := timestampFormatMapper[strings.TrimSpace(params[0])]; ok {
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
			return fmt.Errorf("invalid precision '%s' in signature '%s'. Expected int 0-6", strings.TrimSpace(params[1]), signature)
		}
		if precision < 0 || precision > 6 {
			return fmt.Errorf("precision must be between 0 and 6. Got '%d'", precision)
		}
	} else {
		precision = defaultPrecision
	}

	t.timestampSignature = fmt.Sprintf("timestamp(%d)", precision)

	t.fieldName = strings.TrimSpace(matches[1])

	return nil
}
