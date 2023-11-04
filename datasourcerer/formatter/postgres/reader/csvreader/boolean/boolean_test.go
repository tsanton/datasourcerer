package boolean_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/postgres/reader/csvreader/boolean"
)

func Test_Boolean(t *testing.T) {
	tests := []struct {
		name                 string
		header               string
		input                string
		expectedHeaderName   string
		expectedWriterOutput string
		expectedError        string
	}{
		{
			name:                 "Test_Boolean_DefaultAnnotation",
			header:               "foo[boolean()]",
			input:                "true",
			expectedHeaderName:   "FOO",
			expectedWriterOutput: "true::BOOLEAN AS FOO",
		},
		{
			name:                 "Test_Boolean_AnnotatedTrue",
			header:               "foo[boolean(T,F)]",
			input:                "T",
			expectedHeaderName:   "FOO",
			expectedWriterOutput: "true::BOOLEAN AS FOO",
		},
		{
			name:                 "Test_Boolean_AnnotatedTrue_WithDefaultFalse_InputTrue",
			header:               "foo[boolean(T)]",
			input:                "T",
			expectedHeaderName:   "FOO",
			expectedWriterOutput: "true::BOOLEAN AS FOO",
		},
		{
			name:                 "Test_Boolean_AnnotatedTrue_WithDefaultFalse_InputFalse",
			header:               "foo[boolean(T)]",
			input:                "false",
			expectedHeaderName:   "FOO",
			expectedWriterOutput: "false::BOOLEAN AS FOO",
		},
		{
			name:                 "Test_Boolean_AnnotatedFalse_WithDefaultTrue_InputTrue",
			header:               "foo[boolean(,F)]",
			input:                "true",
			expectedHeaderName:   "FOO",
			expectedWriterOutput: "true::BOOLEAN AS FOO",
		},
		{
			name:                 "Test_Boolean_AnnotatedFalse_WithDefaultTrue_InputFalse",
			header:               "foo[boolean(,F)]",
			input:                "F",
			expectedHeaderName:   "FOO",
			expectedWriterOutput: "false::BOOLEAN AS FOO",
		},
		{
			name:                 "Test_Boolean_BothAnnotations_InputTrue",
			header:               "foo[boolean(TR,FA)]",
			input:                "TR",
			expectedHeaderName:   "FOO",
			expectedWriterOutput: "true::BOOLEAN AS FOO",
		},
		{
			name:                 "Test_Boolean_BothAnnotations_InputFalse",
			header:               "foo[boolean(TR,FA)]",
			input:                "FA",
			expectedHeaderName:   "FOO",
			expectedWriterOutput: "false::BOOLEAN AS FOO",
		},
		{
			name:          "Test_Boolean_Exception_InvalidValue",
			header:        "foo[boolean(TRUE,FALSE)]",
			input:         "not-a-bool",
			expectedError: "invalid boolean value 'not-a-bool', expected 'TRUE' (true) or 'FALSE' (false)",
		},
		{
			name:          "Test_Boolean_Exception_ExtraOpeningParenthesis",
			header:        "foo[boolean((T,F)]",
			expectedError: "unbalanced parentheses in signature 'foo[boolean((T,F)]'",
		},
		{
			name:          "Test_Boolean_Exception_ExtraClosingParenthesis",
			header:        "foo[boolean(T,F))]",
			expectedError: "unbalanced parentheses in signature 'foo[boolean(T,F))]'",
		},
		{
			name:          "Test_Boolean_Exception_MissingClosingParenthesis",
			header:        "foo[boolean(T,F]",
			expectedError: "unbalanced parentheses in signature 'foo[boolean(T,F]'",
		},
		{
			name:          "Test_Boolean_Exception_WrongType",
			header:        "foo[integer(T,F)]",
			expectedError: "invalid signature 'foo[integer(T,F)]'. Expected () or (<optional-true-value>,<optional-false-value>)",
		},
		{
			name:          "Test_Boolean_Exception_ExtraContentOutsideParenthesis",
			header:        "foo[boolean(T,F)]ExtraContent",
			expectedError: "invalid signature 'foo[boolean(T,F)]ExtraContent'. Signature should be of the form <name>[boolean(<optional-true-value>,<optional-false-value>)]",
		},
		{
			name:          "Test_Boolean_Exception_ExtraComma",
			header:        "foo[boolean(T,F,)]",
			expectedError: "invalid signature 'foo[boolean(T,F,)]'. Expected () or (<optional-true-value>,<optional-false-value>)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := boolean.Boolean{}
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
