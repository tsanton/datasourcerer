package jsonb_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/postgres/reader/csvreader/jsonb"
)

func Test_Jsonb(t *testing.T) {
	tests := []struct {
		name                 string
		header               string
		input                string
		expectedHeaderName   string
		expectedWriterOutput string
		expectedError        string
	}{
		{
			name:                 "Test_Jsonb_DefaultAnnotation",
			header:               "foo[jsonb()]",
			input:                `{"foo":"bar"}`,
			expectedHeaderName:   "foo",
			expectedWriterOutput: `'{"foo":"bar"}'::jsonb as foo`,
		},
		{
			name:                 "Test_Jsonb_NoAnnotation",
			header:               "Bar[jsonb()]",
			input:                `{"foo":"bar"}`,
			expectedHeaderName:   "Bar",
			expectedWriterOutput: `'{"foo":"bar"}'::jsonb as Bar`,
		},
		{
			name:                 "Test_Jsonb_AnnotationCaseInsensitive",
			header:               "qUx[JSONb()]",
			input:                `{"foo":"bar"}`,
			expectedHeaderName:   "qUx",
			expectedWriterOutput: `'{"foo":"bar"}'::jsonb as qUx`,
		},

		{
			name:          "Test_Jsonb_Excepton_OneExtraComma",
			header:        "foo[jsonb(,)]",
			expectedError: "invalid signature 'foo[jsonb(,)]'. Expected ()",
		},
		{
			name:          "Test_Jsonb_Exception_ExtraOpeningParenthesis",
			header:        "foo[jsonb(()]",
			expectedError: "unbalanced parentheses in signature 'foo[jsonb(()]'",
		},
		{
			name:          "Test_Jsonb_Exception_ExtraClosingParenthesis",
			header:        "foo[jsonb())]",
			expectedError: "unbalanced parentheses in signature 'foo[jsonb())]'",
		},
		{
			name:          "Test_Jsonb_Exception_MissingOpeningParenthesis",
			header:        "foo[text)]",
			expectedError: "unbalanced parentheses in signature 'foo[text)]'",
		},
		{
			name:          "Test_Jsonb_Exception_MissingClosingParenthesis",
			header:        "foo[jsonb(]",
			expectedError: "unbalanced parentheses in signature 'foo[jsonb(]'",
		},
		{
			name:          "Test_Jsonb_Exception_ExtraContentOutsideParenthesiss",
			header:        "foo[jsonb()]ExtraContent",
			expectedError: "invalid signature 'foo[jsonb()]ExtraContent'. Signature should be of the form <name>[jsonb()]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := jsonb.Jsonb{}
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
