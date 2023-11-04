package bigint

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/tsanton/dbt-unit-test-fusionizer/formatter"
)

var _ formatter.ICsvHeader = &BigInt{}

// Signature must contains "[varchar" (case insensitive) at any position and ends with ")]"
var bigintSignatureRegex = regexp.MustCompile(`^(\w+)\[bigint\((.*?)\)\]$`)

const (
	PostgresBigintSignaturePrefix = "[bigint("
)

// BigInt is signified with "[bigint()]".
type BigInt struct {
	fieldName string
}

// GetName implements formatter.ICsvHeader
func (m *BigInt) GetName() string {
	return m.fieldName
}

// GetWriter implements formatter.ICsvHeader.
func (v *BigInt) GetWriter() func(value interface{}) ([]byte, error) {
	return func(value interface{}) ([]byte, error) {
		val, err := strconv.ParseInt(value.(string), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("error converting value '%s' to integer", value.(string))
		}
		return []byte(fmt.Sprintf("%d::bigint as %s", val, v.fieldName)), nil
	}
}

func (v *BigInt) ParseHeader(signature string) error {
	if !strings.HasSuffix(signature, "]") {
		return fmt.Errorf("invalid signature '%s'. Signature should be of the form <name>[bigint()]", signature)
	}

	if count := strings.Count(signature, "(") - strings.Count(signature, ")"); count != 0 {
		return fmt.Errorf("unbalanced parentheses in signature '%s'", signature)
	}

	// Extract the regex matches
	matches := bigintSignatureRegex.FindStringSubmatch(signature)

	if len(matches) != 3 {
		return fmt.Errorf("invalid signature '%s'. Expected ()", signature)
	}

	if !strings.HasSuffix(signature, "]") {
		return fmt.Errorf("invalid signature '%s'. Signature should be of the form <name>[bigint()]", signature)
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
