package number_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/snowflake/reader/csvreader/number"
)

func Test_Number(t *testing.T) {
	tests := []struct {
		name                 string
		header               string
		input                string
		expectedHeaderName   string
		expectedWriterOutput string
		expectedError        string
	}{
		{
			name:                 "Test_Number_DefaultAnnotation",
			header:               "foo[number()]",
			input:                "12.2",
			expectedHeaderName:   "FOO",
			expectedWriterOutput: "12.2::NUMBER(38,2) AS FOO",
		},
		{
			name:                 "Test_Integer_Annotated",
			header:               "foo[number(14,0)]",
			input:                "10",
			expectedHeaderName:   "FOO",
			expectedWriterOutput: "10::NUMBER(14,0) AS FOO",
		},
		{
			name:                 "Test_Number_AnnotatedPrecision_DefaultScale",
			header:               "foo[number(10)]",
			input:                "12.2",
			expectedHeaderName:   "FOO",
			expectedWriterOutput: "12.2::NUMBER(10,2) AS FOO",
		},
		{
			name:                 "Test_Number_DefaultPrecision_AnnotatedScale",
			header:               "foo[number(,7)]",
			input:                "12.2",
			expectedHeaderName:   "FOO",
			expectedWriterOutput: "12.2::NUMBER(38,7) AS FOO",
		},
		{
			name:          "Test_Number_Exception_InvalidPrecision_ValidScale",
			header:        "foo[number(ten, 4)]",
			expectedError: "invalid precision value: 'ten'",
		},
		{
			name:          "Test_Number_Exception_ValidPrecision_InvalidScale",
			header:        "foo[number(10, four)]",
			expectedError: "invalid scale value: 'four'",
		},
		{
			name:          "Test_Number_Exception_InvalidPrecision_ValidScale",
			header:        "foo[number(ten, four)]",
			expectedError: "invalid precision value: 'ten'",
		},
		{
			name:          "Test_Number_Exception_InvalidInteger",
			header:        "foo[number(4,0)]",
			input:         "not-a-number",
			expectedError: "error converting value 'not-a-number' to integer",
		},
		{
			name:          "Test_Number_Exception_InvalidFloat",
			header:        "foo[number(4,2)]",
			input:         "not-a-number",
			expectedError: "error converting value 'not-a-number' to float",
		},
		{
			name:          "Test_Number_Exception_WrongType",
			header:        "foo[integer(14,3)]",
			expectedError: "invalid signature 'foo[integer(14,3)]'. Expected () or (<optional-precision>,<optional-scale>)",
		},
		{
			name:          "Test_Number_Exception_ExtraComma",
			header:        "foo[number(14,3,)]",
			expectedError: "invalid signature 'foo[number(14,3,)]'. Expected () or (<optional-precision>,<optional-scale>)",
		},
		{
			name:          "Test_Number_Exception_ExtraOpeningParenthesis",
			header:        "foo[number((14,3)]",
			expectedError: "unbalanced parentheses in signature 'foo[number((14,3)]'",
		},
		{
			name:          "Test_Number_Exception_ExtraClosingParenthesis",
			header:        "foo[number(14,3))]",
			expectedError: "unbalanced parentheses in signature 'foo[number(14,3))]'",
		},
		{
			name:          "Test_Number_Exception_MissingOpeningParenthesis",
			header:        "foo[number14,3)]",
			expectedError: "unbalanced parentheses in signature 'foo[number14,3)]'",
		},
		{
			name:          "Test_Number_Exception_MissingClosingParenthesis",
			header:        "foo[number(14,3]",
			expectedError: "unbalanced parentheses in signature 'foo[number(14,3]'",
		},
		{
			name:          "Test_Number_Exception_ExtraContentOutsideParenthesiss",
			header:        "foo[number(14,3)]ExtraContent",
			expectedError: "invalid signature 'foo[number(14,3)]ExtraContent'. Signature should be of the form <name>[number(<optional-precision>,<optional-scale>)]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := number.Number{}
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
