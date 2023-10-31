package varchar

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/tsanton/dbt-unit-test-fusionizer/formatter"
)

var _ formatter.ICsvHeader = &Varchar{}

// Signature must contains "[varchar" (case insensitive) at any position and ends with ")]"
var varcharSignatureRegex = regexp.MustCompile(`^(\w+)\[varchar\((.*?)\)\]$`)

const (
	SnowflakeVarcharSignaturePrefix = "[varchar("
	defaultBytes                    = 16777216
)

// Varchar are signified with "[varchar]". It is also default if no [<type>] is spesified
type Varchar struct {
	fieldName string
	bytes     int
}

// GetName implements formatter.ICsvHeader
func (m *Varchar) GetName() string {
	return m.fieldName
}

// TODO: refactor
// GetWriter implements formatter.ICsvHeader.
func (v *Varchar) GetWriter() func(value interface{}) ([]byte, error) {
	return func(value interface{}) ([]byte, error) {
		return []byte(fmt.Sprintf("'%s'::VARCHAR(%d) AS %s", value, v.bytes, strings.ToUpper(v.fieldName))), nil
	}
}

// TODO: refactor
func (v *Varchar) ParseHeader(signature string) error {
	matches := varcharSignatureRegex.FindStringSubmatch(signature)
	//Varchar must be handled a bit differently because it is the default type if no annotation is specified
	if len(matches) != 3 && !strings.Contains(signature, "[") && !strings.Contains(signature, "]") {
		v.fieldName = strings.ToUpper(strings.TrimSpace(signature))
		v.bytes = defaultBytes
		return nil
	}

	if !strings.HasSuffix(signature, "]") {
		return fmt.Errorf("invalid signature '%s'. Signature should be of the form <name>[varchar(<optional-bytes>)]", signature)
	}

	if count := strings.Count(signature, "(") - strings.Count(signature, ")"); count != 0 {
		return fmt.Errorf("unbalanced parentheses in signature '%s'", signature)
	}

	args := strings.Count(matches[2], ",")
	if args > 0 {
		return fmt.Errorf("invalid signature '%s'. Expected () or (<optional-bytes>)", signature)
	}

	params := strings.Split(matches[2], ",")

	// Parse optional bytes
	if len(params) > 0 && strings.TrimSpace(params[0]) != "" {
		bytes, err := strconv.Atoi(strings.TrimSpace(params[0]))
		if err != nil {
			return fmt.Errorf("invalid bytes '%s' in signature '%s'. Expected int 0-16777216", strings.TrimSpace(params[0]), signature)
		}
		if bytes < 0 || bytes > 16777216 {
			return fmt.Errorf("bytes must be between 0 and 16777216. Got '%d'", bytes)
		}
		v.bytes = bytes
	} else {
		v.bytes = defaultBytes
	}

	v.fieldName = strings.ToUpper(strings.TrimSpace(matches[1]))
	return nil
}
