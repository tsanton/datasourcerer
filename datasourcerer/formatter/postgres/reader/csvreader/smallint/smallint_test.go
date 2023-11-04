package smallint_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/postgres/reader/csvreader/smallint"
)

func Test_smallint(t *testing.T) {
	tests := []struct {
		name                 string
		header               string
		input                string
		expectedHeaderName   string
		expectedWriterOutput string
		expectedError        string
	}{
		{
			name:                 "Test_Smallint_Annotated",
			header:               "foo[smallint()]",
			input:                "10",
			expectedHeaderName:   "foo",
			expectedWriterOutput: "10::smallint as foo",
		},
		{
			name:          "Test_Smallint_Exception_InvalidInteger",
			header:        "foo[smallint()]",
			input:         "not-a-number",
			expectedError: "error converting value 'not-a-number' to integer",
		},
		{
			name:          "Test_Smallint_Exception_IntegerOutOfRangeAbove",
			header:        "foo[smallint()]",
			input:         "2147483649",
			expectedError: "value 2147483649 is out of range for integer, must be in range -32.768 to 32.768",
		},
		{
			name:          "Test_Smallint_Exception_IntegerOutOfRangeBelow",
			header:        "foo[smallint()]",
			input:         "-2147483649",
			expectedError: "value -2147483649 is out of range for integer, must be in range -32.768 to 32.768",
		},
		{
			name:          "Test_Smallint_Exception_OneExtraComma",
			header:        "foo[smallint(,)]",
			expectedError: "invalid signature 'foo[smallint(,)]'. Expected ()",
		},
		{
			name:          "Test_Smallint_Exception_ExtraOpeningParenthesis",
			header:        "foo[smallint(()]",
			expectedError: "unbalanced parentheses in signature 'foo[smallint(()]'",
		},
		{
			name:          "Test_Smallint_Exception_ExtraClosingParenthesis",
			header:        "foo[smallint())]",
			expectedError: "unbalanced parentheses in signature 'foo[smallint())]'",
		},
		{
			name:          "Test_Smallint_Exception_MissingOpeningParenthesis",
			header:        "foo[smallint)]",
			expectedError: "unbalanced parentheses in signature 'foo[smallint)]'",
		},
		{
			name:          "Test_Smallint_Exception_MissingClosingParenthesis",
			header:        "foo[smallint(]",
			expectedError: "unbalanced parentheses in signature 'foo[smallint(]'",
		},
		{
			name:          "Test_Smallint_Exception_ExtraContentOutsideParenthesiss",
			header:        "foo[smallint()]ExtraContent",
			expectedError: "invalid signature 'foo[smallint()]ExtraContent'. Signature should be of the form <name>[smallint()]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := smallint.SmallInt{}
			err := header.ParseHeader(tt.header)

			if tt.expectedError != "" {
				if tt.input != "" && err == nil {
					content, err := header.GetWriter()(tt.input)
					assert.NotNil(t, err)
					assert.EqualError(t, err, tt.expectedError)
					assert.Nil(t, content)
				} else {
					assert.NotNil(t, err)
					assert.EqualError(t, err, tt.expectedError)
				}
			} else {
				assert.Nil(t, err)
				assert.Equal(t, tt.expectedHeaderName, header.GetName())
				content, err := header.GetWriter()(tt.input)
				assert.Nil(t, err)
				assert.Equal(t, tt.expectedWriterOutput, string(content))
			}
		})
	}
}
