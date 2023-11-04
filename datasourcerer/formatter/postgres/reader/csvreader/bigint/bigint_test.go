package bigint_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/postgres/reader/csvreader/bigint"
)

func Test_Bigint(t *testing.T) {
	tests := []struct {
		name                 string
		header               string
		input                string
		expectedHeaderName   string
		expectedWriterOutput string
		expectedError        string
	}{
		{
			name:                 "Test_Bigint_Annotated",
			header:               "foo[bigint()]",
			input:                "10",
			expectedHeaderName:   "foo",
			expectedWriterOutput: "10::bigint as foo",
		},
		{
			name:          "Test_Bigint_Exception_InvalidInteger",
			header:        "foo[bigint()]",
			input:         "not-a-number",
			expectedError: "error converting value 'not-a-number' to integer",
		},
		{
			name:          "Test_Bigint_Exception_OneExtraComma",
			header:        "foo[bigint(,)]",
			expectedError: "invalid signature 'foo[bigint(,)]'. Expected ()",
		},
		{
			name:          "Test_Bigint_Exception_ExtraOpeningParenthesis",
			header:        "foo[bigint(()]",
			expectedError: "unbalanced parentheses in signature 'foo[bigint(()]'",
		},
		{
			name:          "Test_Bigint_Exception_ExtraClosingParenthesis",
			header:        "foo[bigint())]",
			expectedError: "unbalanced parentheses in signature 'foo[bigint())]'",
		},
		{
			name:          "Test_Bigint_Exception_MissingOpeningParenthesis",
			header:        "foo[bigint)]",
			expectedError: "unbalanced parentheses in signature 'foo[bigint)]'",
		},
		{
			name:          "Test_Bigint_Exception_MissingClosingParenthesis",
			header:        "foo[bigint(]",
			expectedError: "unbalanced parentheses in signature 'foo[bigint(]'",
		},
		{
			name:          "Test_Bigint_Exception_ExtraContentOutsideParenthesiss",
			header:        "foo[bigint()]ExtraContent",
			expectedError: "invalid signature 'foo[bigint()]ExtraContent'. Signature should be of the form <name>[bigint()]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := bigint.BigInt{}
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
