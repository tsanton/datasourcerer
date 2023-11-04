package integer

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/tsanton/dbt-unit-test-fusionizer/formatter"
)

var _ formatter.ICsvHeader = &Integer{}

// Signature must contains "[varchar" (case insensitive) at any position and ends with ")]"
var intSignatureRegex = regexp.MustCompile(`^(\w+)\[int\((.*?)\)\]$`)

const (
	PostgresIntegerSignaturePrefix = "[int("
)

// Integer is signified with "[text()]". It is also default if no [<type>] is spesified
type Integer struct {
	fieldName string
}

// GetName implements formatter.ICsvHeader
func (m *Integer) GetName() string {
	return m.fieldName
}

// GetWriter implements formatter.ICsvHeader.
func (v *Integer) GetWriter() func(value interface{}) ([]byte, error) {
	return func(value interface{}) ([]byte, error) {
		_, err := strconv.ParseInt(value.(string), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("error converting value '%s' to integer", value.(string))
		}
		return []byte(fmt.Sprintf("%v::int as %s", value, v.fieldName)), nil
	}
}

func (v *Integer) ParseHeader(signature string) error {
	if !strings.HasSuffix(signature, "]") {
		return fmt.Errorf("invalid signature '%s'. Signature should be of the form <name>[int()]", signature)
	}

	if count := strings.Count(signature, "(") - strings.Count(signature, ")"); count != 0 {
		return fmt.Errorf("unbalanced parentheses in signature '%s'", signature)
	}

	// Extract the regex matches
	matches := intSignatureRegex.FindStringSubmatch(signature)

	if len(matches) != 3 {
		return fmt.Errorf("invalid signature '%s'. Expected ()", signature)
	}

	if !strings.HasSuffix(signature, "]") {
		return fmt.Errorf("invalid signature '%s'. Signature should be of the form <name>[int()]", signature)
	}

	if count := strings.Count(signature, "(") - strings.Count(signature, ")"); count != 0 {
		return fmt.Errorf("unbalanced parentheses in signature '%s'", signature)
	}

	args := strings.Count(matches[2], ",")
	if args != 0 {
		return fmt.Errorf("invalid signature '%s'. Expected ()", signature)
	}

	v.fieldName = strings.TrimSpace(matches[1])
	return nil
}
