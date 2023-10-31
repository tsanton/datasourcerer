package number

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/tsanton/dbt-unit-test-fusionizer/formatter"
)

var _ formatter.ICsvHeader = &Number{}

// Signature must contains "[number" (case insensitive) at any position and ends with ")]"
var snowflakeNumberSignatureRegex = regexp.MustCompile(`^(\w+)\[number\((.*?)\)\]$`)

const (
	SnowflakeNumberSignaturePrefix = "[number("
	defaultPrecision               = 38
	defaultScale                   = 2
)

type Number struct {
	fieldName string
	precision int
	scale     int
}

// GetName implements formatter.ICsvHeader
func (m *Number) GetName() string {
	return m.fieldName
}

// GetWriter implements formatter.ICsvHeader.
func (n *Number) GetWriter() func(value interface{}) ([]byte, error) {
	return func(value interface{}) ([]byte, error) {
		if n.scale == 0 {
			_, err := strconv.ParseInt(value.(string), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("error converting value '%s' to integer", value.(string))
			}
		} else {
			_, err := strconv.ParseFloat(value.(string), 64)
			if err != nil {
				return nil, fmt.Errorf("error converting value '%s' to float", value.(string))
			}
		}
		return []byte(fmt.Sprintf("%v::NUMBER(%d,%d) AS %s", value, n.precision, n.scale, strings.ToUpper(n.fieldName))), nil
	}
}

// GetWriter implements formatter.ICsvHeader.
func (n *Number) ParseHeader(signature string) error {
	var err error
	if !strings.HasSuffix(signature, "]") {
		return fmt.Errorf("invalid signature '%s'. Signature should be of the form <name>[number(<optional-precision>,<optional-scale>)]", signature)
	}

	if count := strings.Count(signature, "(") - strings.Count(signature, ")"); count != 0 {
		return fmt.Errorf("unbalanced parentheses in signature '%s'", signature)
	}

	// Extract the regex matches
	matches := snowflakeNumberSignatureRegex.FindStringSubmatch(signature)

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
		n.precision, err = strconv.Atoi(strings.TrimSpace(params[0]))
		if err != nil {
			return fmt.Errorf("invalid precision value: '%s'", strings.TrimSpace(params[0]))
		}
	} else {
		n.precision = defaultPrecision
	}

	// Parse optional scale
	if len(params) > 1 && strings.TrimSpace(params[1]) != "" {
		n.scale, err = strconv.Atoi(strings.TrimSpace(params[1]))
		if err != nil {
			return fmt.Errorf("invalid scale value: '%s'", strings.TrimSpace(params[1]))
		}
	} else {
		n.scale = defaultScale
	}

	n.fieldName = strings.ToUpper(strings.TrimSpace(matches[1]))

	return nil
}
