package integer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/postgres/reader/csvreader/integer"
)

func Test_int(t *testing.T) {
	tests := []struct {
		name                 string
		header               string
		input                string
		expectedHeaderName   string
		expectedWriterOutput string
		expectedError        string
	}{
		{
			name:                 "Test_Integer_Annotated",
			header:               "foo[int()]",
			input:                "10",
			expectedHeaderName:   "foo",
			expectedWriterOutput: "10::int as foo",
		},
		{
			name:          "Test_Integer_Exception_InvalidInteger",
			header:        "foo[int()]",
			input:         "not-a-number",
			expectedError: "error converting value 'not-a-number' to integer",
		},
		{
			name:          "Test_Integer_Exception_OneExtraComma",
			header:        "foo[int(,)]",
			expectedError: "invalid signature 'foo[int(,)]'. Expected ()",
		},
		{
			name:          "Test_Integer_Exception_ExtraOpeningParenthesis",
			header:        "foo[int(()]",
			expectedError: "unbalanced parentheses in signature 'foo[int(()]'",
		},
		{
			name:          "Test_Integer_Exception_ExtraClosingParenthesis",
			header:        "foo[int())]",
			expectedError: "unbalanced parentheses in signature 'foo[int())]'",
		},
		{
			name:          "Test_Integer_Exception_MissingOpeningParenthesis",
			header:        "foo[int)]",
			expectedError: "unbalanced parentheses in signature 'foo[int)]'",
		},
		{
			name:          "Test_Integer_Exception_MissingClosingParenthesis",
			header:        "foo[int(]",
			expectedError: "unbalanced parentheses in signature 'foo[int(]'",
		},
		{
			name:          "Test_Integer_Exception_ExtraContentOutsideParenthesiss",
			header:        "foo[int()]ExtraContent",
			expectedError: "invalid signature 'foo[int()]ExtraContent'. Signature should be of the form <name>[int()]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := integer.Integer{}
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
