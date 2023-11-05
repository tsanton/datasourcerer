package jsonb

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/tsanton/dbt-unit-test-fusionizer/formatter"
)

var _ formatter.ICsvHeader = &Jsonb{}

// Signature must contains "[jsonb" (case insensitive) at any position and ends with ")]"
var jsonbSignatureRegex = regexp.MustCompile(`(?i)^(\w+)\[jsonb\((.*?)\)\]$`)

const (
	PostgresJsonbSignaturePrefix = "[jsonb("
)

// Jsonb is signified with "[jsonb()]". It is also default if no [<type>] is spesified
type Jsonb struct {
	fieldName string
}

// GetName implements formatter.ICsvHeader
func (m *Jsonb) GetName() string {
	return m.fieldName
}

// GetWriter implements formatter.ICsvHeader.
func (v *Jsonb) GetWriter() func(value interface{}) ([]byte, error) {
	return func(value interface{}) ([]byte, error) {
		return []byte(fmt.Sprintf("'%s'::jsonb as %s", value, v.fieldName)), nil
	}
}

func (v *Jsonb) ParseHeader(signature string) error {
	if !strings.HasSuffix(signature, "]") {
		return fmt.Errorf("invalid signature '%s'. Signature should be of the form <name>[jsonb()]", signature)
	}

	if count := strings.Count(signature, "(") - strings.Count(signature, ")"); count != 0 {
		return fmt.Errorf("unbalanced parentheses in signature '%s'", signature)
	}

	matches := jsonbSignatureRegex.FindStringSubmatch(signature)
	if len(matches) != 3 && !strings.Contains(signature, "[") && !strings.Contains(signature, "]") {
		v.fieldName = strings.TrimSpace(signature)
		return nil
	}

	if !strings.HasSuffix(signature, "]") {
		return fmt.Errorf("invalid signature '%s'. Signature should be of the form <name>[text()]", signature)
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
