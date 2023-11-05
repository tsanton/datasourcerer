package date

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/tsanton/dbt-unit-test-fusionizer/formatter"
)

var _ formatter.ICsvHeader = &Date{}

// Signature must contains "[date" (case insensitive) at any position and ends with ")]"
var dateSignatureRegex = regexp.MustCompile(`(?i)^(\w+)\[date\((.*?)\)\]$`)

const (
	PostgresDateSignaturePrefix = "[date("
	defaultDateFormat           = "2006-01-02"
)

type Date struct {
	fieldName string
	format    string
}

var dateFormatMapper = map[string]string{
	"yyyy-MM-dd":           "2006-01-02",                // Example: "2023-10-24"
	"dd-MM-yyyy":           "02-01-2006",                // Example: "24-10-2023"
	"MM/dd/yyyy":           "01/02/2006",                // Example: "10/24/2023"
	"yyyy/MM/dd":           "2006/01/02",                // Example: "2023/10/24"
	"dd/MM/yyyy":           "02/01/2006",                // Example: "24/10/2023"
	"MMM dd, yyyy":         "Jan 02, 2006",              // Example: "Oct 24, 2023"
	"MMMM dd, yyyy":        "January 02, 2006",          // Example: "October 24, 2023"
	"dd MMM yyyy":          "02 Jan 2006",               // Example: "24 Oct 2023"
	"yyyy-MM-ddTHH:mm:ssZ": "2006-01-02T15:04:05Z07:00", // Example: "2023-10-24T00:00:00Z"
}

// GetName implements formatter.ICsvHeader
func (m *Date) GetName() string {
	return m.fieldName
}

// GetWriter implements formatter.ICsvHeader.
func (d *Date) GetWriter() func(value interface{}) ([]byte, error) {
	return func(value interface{}) ([]byte, error) {
		t, err := time.Parse(d.format, value.(string))
		if err != nil {
			return nil, fmt.Errorf("not able to convert value '%s' to date using the '%s' format", value.(string), d.format)
		}
		return []byte(fmt.Sprintf("'%s'::date as %s", t.Format(defaultDateFormat), d.fieldName)), nil
	}
}

// TODO: refactor
// GetWriter implements formatter.ICsvHeader.
func (d *Date) ParseHeader(signature string) error {
	if !strings.HasSuffix(signature, "]") {
		return fmt.Errorf("invalid signature '%s'. Signature should be of the form <name>[date(<format>)]", signature)
	}

	// Check for unbalanced parentheses
	if count := strings.Count(signature, "(") - strings.Count(signature, ")"); count != 0 {
		return fmt.Errorf("unbalanced parentheses in signature '%s'", signature)
	}

	// Extract the regex matches// Extract the regex matches
	matches := dateSignatureRegex.FindStringSubmatch(signature)

	if len(matches) != 3 {
		return fmt.Errorf("invalid signature '%s'. Expected () or (<format>)", signature)
	}

	args := strings.Count(matches[2], ",")
	if args > 0 {
		return fmt.Errorf("invalid signature '%s'. Expected () or (<format>)", signature)
	}

	params := strings.Split(matches[2], ",")

	// Parse optional format
	if len(params) > 0 && strings.TrimSpace(params[0]) != "" {
		if format, ok := dateFormatMapper[strings.TrimSpace(params[0])]; ok {
			d.format = format
		} else {
			d.format = strings.TrimSpace(params[0])
		}
	} else {
		d.format = defaultDateFormat
	}

	d.fieldName = strings.TrimSpace(matches[1])

	return nil
}
