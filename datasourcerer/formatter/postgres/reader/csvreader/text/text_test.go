package text_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/postgres/reader/csvreader/text"
)

func Test_Text(t *testing.T) {
	tests := []struct {
		name                 string
		header               string
		input                string
		expectedHeaderName   string
		expectedWriterOutput string
		expectedError        string
	}{
		{
			name:                 "Test_Text_NoAnnotation",
			header:               "Bar",
			input:                "bar",
			expectedHeaderName:   "Bar",
			expectedWriterOutput: "'bar'::text as Bar",
		},
		{
			name:                 "Test_Text_DefaultAnnotation",
			header:               "foo[text()]",
			input:                "bar",
			expectedHeaderName:   "foo",
			expectedWriterOutput: "'bar'::text as foo",
		},
		{
			name:                 "Test_Text_AnnotationCaseInsensitive",
			header:               "qUx[TexT()]",
			input:                "bar",
			expectedHeaderName:   "qUx",
			expectedWriterOutput: "'bar'::text as qUx",
		},
		{
			name:          "Test_Text_Excepton_OneExtraComma",
			header:        "foo[text(,)]",
			expectedError: "invalid signature 'foo[text(,)]'. Expected ()",
		},
		{
			name:          "Test_Text_Exception_ExtraOpeningParenthesis",
			header:        "foo[text(()]",
			expectedError: "unbalanced parentheses in signature 'foo[text(()]'",
		},
		{
			name:          "Test_Text_Exception_ExtraClosingParenthesis",
			header:        "foo[text())]",
			expectedError: "unbalanced parentheses in signature 'foo[text())]'",
		},
		{
			name:          "Test_Text_Exception_MissingOpeningParenthesis",
			header:        "foo[text)]",
			expectedError: "unbalanced parentheses in signature 'foo[text)]'",
		},
		{
			name:          "Test_Text_Exception_MissingClosingParenthesis",
			header:        "foo[text(]",
			expectedError: "unbalanced parentheses in signature 'foo[text(]'",
		},
		{
			name:          "Test_Text_Exception_ExtraContentOutsideParenthesiss",
			header:        "foo[text()]ExtraContent",
			expectedError: "invalid signature 'foo[text()]ExtraContent'. Signature should be of the form <name>[text()]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := text.Text{}
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
