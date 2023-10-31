package varchar_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/snowflake/reader/csvreader/varchar"
)

func Test_Varchar(t *testing.T) {
	tests := []struct {
		name                 string
		header               string
		input                string
		expectedHeaderName   string
		expectedWriterOutput string
		expectedError        string
	}{
		{
			name:                 "Test_Varchar_DefaultAnnotation",
			header:               "foo[varchar()]",
			input:                "bar",
			expectedHeaderName:   "FOO",
			expectedWriterOutput: "'bar'::VARCHAR(16777216) AS FOO",
		},
		{
			name:                 "Test_Varchar_NoAnnotation",
			header:               "foo",
			input:                "bar",
			expectedHeaderName:   "FOO",
			expectedWriterOutput: "'bar'::VARCHAR(16777216) AS FOO",
		},
		{
			name:                 "Test_Varchar_ByteAnnotated",
			header:               "foo[varchar(10)]",
			input:                "bar",
			expectedHeaderName:   "FOO",
			expectedWriterOutput: "'bar'::VARCHAR(10) AS FOO",
		},
		{
			name:          "Test_Varchar_Excepton_OneExtraComma",
			header:        "foo[varchar(,)]",
			expectedError: "invalid signature 'foo[varchar(,)]'. Expected () or (<optional-bytes>)",
		},
		{
			name:          "Test_Varchar_Excepton_InvalidBytesNonNumeric",
			header:        "foo[varchar(abc)]",
			expectedError: "invalid bytes 'abc' in signature 'foo[varchar(abc)]'. Expected int 0-16777216",
		},
		{
			name:          "Test_Varchar_Excepton_InvalidBytesOutOfRange",
			header:        "foo[varchar(16777217)]",
			expectedError: "bytes must be between 0 and 16777216. Got '16777217'",
		},
		{
			name:          "Test_Varchar_Exception_ExtraOpeningParenthesis",
			header:        "foo[varchar(()]",
			expectedError: "unbalanced parentheses in signature 'foo[varchar(()]'",
		},
		{
			name:          "Test_Varchar_Exception_ExtraClosingParenthesis",
			header:        "foo[varchar())]",
			expectedError: "unbalanced parentheses in signature 'foo[varchar())]'",
		},
		{
			name:          "Test_Varchar_Exception_MissingOpeningParenthesis",
			header:        "foo[varchar)]",
			expectedError: "unbalanced parentheses in signature 'foo[varchar)]'",
		},
		{
			name:          "Test_Varchar_Exception_MissingClosingParenthesis",
			header:        "foo[varchar(]",
			expectedError: "unbalanced parentheses in signature 'foo[varchar(]'",
		},
		{
			name:          "Test_Varchar_Exception_ExtraContentOutsideParenthesiss",
			header:        "foo[varchar()]ExtraContent",
			expectedError: "invalid signature 'foo[varchar()]ExtraContent'. Signature should be of the form <name>[varchar(<optional-bytes>)]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := varchar.Varchar{}
			err := header.ParseHeader(tt.header)

			if tt.expectedError != "" {
				assert.NotNil(t, err)
				assert.EqualError(t, err, tt.expectedError)
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
