package numeric

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/tsanton/dbt-unit-test-fusionizer/formatter"
)

var _ formatter.ICsvHeader = &Numeric{}

// Signature must contains "[numeric" (case insensitive) at any position and ends with ")]"
var numericSignatureRegex = regexp.MustCompile(`^(\w+)\[numeric\((.*?)\)\]$`)

const (
	PostgresNumericSignaturePrefix = "[numeric("
)

type Numeric struct {
	fieldName string
	precision int
	scale     int
}

// GetName implements formatter.ICsvHeader
func (m *Numeric) GetName() string {
	return m.fieldName
}

// GetWriter implements formatter.ICsvHeader.
func (n *Numeric) GetWriter() func(value interface{}) ([]byte, error) {
	return func(value interface{}) ([]byte, error) {
		_, err := strconv.ParseFloat(value.(string), 64)
		if err != nil {
			return nil, fmt.Errorf("error converting value '%s' to float", value.(string))
		}
		if n.precision == -99999 && n.scale == -99999 {
			return []byte(fmt.Sprintf("%s::numeric as %s", value.(string), n.fieldName)), nil
		} else {
			if n.precision == -99999 {
				return nil, fmt.Errorf("precision must be spesified along with scale")
			}
			if n.scale == -99999 {
				return nil, fmt.Errorf("scale must be spesified along with precision")
			}
			if n.precision > 1000 || n.precision < 0 {
				return nil, fmt.Errorf("invalid precision value: '%d', must be in range 0-1000", n.precision)
			}
			if n.scale > 999 || n.scale < 0 || n.scale > n.precision {
				return nil, fmt.Errorf("invalid scale value: '%d', must be smaller than precision value '%d'", n.precision, n.scale)
			}
			return []byte(fmt.Sprintf("%s::numeric(%d,%d) as %s", value.(string), n.precision, n.scale, n.fieldName)), nil
		}
	}
}

// GetWriter implements formatter.ICsvHeader.
func (n *Numeric) ParseHeader(signature string) error {
	var err error
	if !strings.HasSuffix(signature, "]") {
		return fmt.Errorf("invalid signature '%s'. Signature should be of the form <name>[numeric(<optional-precision>,<optional-scale>)]", signature)
	}

	if count := strings.Count(signature, "(") - strings.Count(signature, ")"); count != 0 {
		return fmt.Errorf("unbalanced parentheses in signature '%s'", signature)
	}

	// Extract the regex matches
	matches := numericSignatureRegex.FindStringSubmatch(signature)

	if len(matches) != 3 {
		return fmt.Errorf("invalid signature '%s'. Expected () or (<optional-precision>,<optional-scale>)", signature)
	}

	// Count arguments in parentheses
	args := strings.Count(matches[2], ",")
	if args > 1 {
		return fmt.Errorf("invalid signature '%s'. Expected () or (<optional-precision>,<optional-scale>)", signature)
	}

	params := strings.Split(matches[2], ",")

	// Parse optional precision
	if len(params) > 0 && strings.TrimSpace(params[0]) != "" {
		precision, err := strconv.Atoi(strings.TrimSpace(params[0]))
		if err != nil {
			return fmt.Errorf("invalid precision value: '%s'", strings.TrimSpace(params[0]))
		}
		n.precision = precision
	} else {
		n.precision = -99999
	}

	// Parse optional scale
	if len(params) > 1 && strings.TrimSpace(params[1]) != "" {
		n.scale, err = strconv.Atoi(strings.TrimSpace(params[1]))
		if err != nil {
			return fmt.Errorf("invalid scale value: '%s'", strings.TrimSpace(params[1]))
		}
	} else {
		n.scale = -99999
	}

	n.fieldName = strings.TrimSpace(matches[1])

	return nil
}
