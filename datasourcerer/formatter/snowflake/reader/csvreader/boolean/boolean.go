package boolean

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/tsanton/dbt-unit-test-fusionizer/formatter"
)

var _ formatter.ICsvHeader = &Boolean{}

// Signature must contains "[boolean" (case insensitive) at any position and ends with ")]"
var snowflakeBooleanSignatureRegex = regexp.MustCompile(`^(\w+)\[boolean\((.*?)\)\]$`)

const SnowflakeBooleanSignaturePrefix = "[boolean("

const (
	defaultTrue  = "true"
	defaultFalse = "false"
)

// CustomBool are signified with "[boolean(x,y)]" where x == the true value and y == the false value (i.e. "y" represents true and "n" reoresents no.)
type Boolean struct {
	fieldName           string
	trueRepresentation  string //defaults to "true"
	falseRepresentation string //defaults to "false"
}

// GetName implements formatter.ICsvHeader
func (m *Boolean) GetName() string {
	return m.fieldName
}

// GetWriter implements formatter.ICsvHeader.
func (b *Boolean) GetWriter() func(value interface{}) ([]byte, error) {
	return func(value interface{}) ([]byte, error) {
		var val string
		if b.trueRepresentation == value {
			val = fmt.Sprint(true)
		} else if b.falseRepresentation == value {
			val = fmt.Sprint(false)
		} else {
			return nil, fmt.Errorf("invalid boolean value '%s', expected '%s' (true) or '%s' (false)", value, b.trueRepresentation, b.falseRepresentation)
		}
		return []byte(fmt.Sprintf("%s::BOOLEAN AS %s", val, strings.ToUpper(b.fieldName))), nil
	}
}

// TODO: refactor
// GetWriter implements formatter.ICsvHeader.
func (b *Boolean) ParseHeader(signature string) error {
	if !strings.HasSuffix(signature, "]") {
		return fmt.Errorf("invalid signature '%s'. Signature should be of the form <name>[boolean(<optional-true-value>,<optional-false-value>)]", signature)
	}
	// Check for unbalanced parentheses
	if count := strings.Count(signature, "(") - strings.Count(signature, ")"); count != 0 {
		return fmt.Errorf("unbalanced parentheses in signature '%s'", signature)
	}

	// Extract the regex matches// Extract the regex matches
	matches := snowflakeBooleanSignatureRegex.FindStringSubmatch(signature)

	if len(matches) != 3 {
		return fmt.Errorf("invalid signature '%s'. Expected () or (<optional-true-value>,<optional-false-value>)", signature)
	}

	args := strings.Count(matches[2], ",")
	if args > 1 {
		return fmt.Errorf("invalid signature '%s'. Expected () or (<optional-true-value>,<optional-false-value>)", signature)
	}

	params := strings.Split(matches[2], ",")

	// Parse optional true value
	if len(params) > 0 && strings.TrimSpace(params[0]) != "" {
		b.trueRepresentation = strings.TrimSpace(params[0])
	} else {
		b.trueRepresentation = defaultTrue
	}

	// Parse optional false value
	if len(params) > 1 && strings.TrimSpace(params[1]) != "" {
		b.falseRepresentation = strings.TrimSpace(params[1])
	} else {
		b.falseRepresentation = defaultFalse
	}

	b.fieldName = strings.ToUpper(strings.TrimSpace(matches[1]))

	return nil
}
