package text

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/tsanton/dbt-unit-test-fusionizer/formatter"
)

var _ formatter.ICsvHeader = &Text{}

// Signature must contains "[varchar" (case insensitive) at any position and ends with ")]"
var textSignatureRegex = regexp.MustCompile(`^(\w+)\[text\((.*?)\)\]$`)

const (
	PostgresTextSignaturePrefix = "[text("
)

// Text is signified with "[text()]". It is also default if no [<type>] is spesified
type Text struct {
	fieldName string
}

// GetName implements formatter.ICsvHeader
func (m *Text) GetName() string {
	return m.fieldName
}

// GetWriter implements formatter.ICsvHeader.
func (v *Text) GetWriter() func(value interface{}) ([]byte, error) {
	return func(value interface{}) ([]byte, error) {
		return []byte(fmt.Sprintf("'%s'::text as %s", value, v.fieldName)), nil
	}
}

// TODO: refactor
func (v *Text) ParseHeader(signature string) error {
	matches := textSignatureRegex.FindStringSubmatch(signature)
	//Varchar must be handled a bit differently because it is the default type if no annotation is specified
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
