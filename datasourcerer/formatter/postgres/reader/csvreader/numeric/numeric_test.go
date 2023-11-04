package numeric_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/postgres/reader/csvreader/numeric"
)

func Test_Numeric(t *testing.T) {
	tests := []struct {
		name                 string
		header               string
		input                string
		expectedHeaderName   string
		expectedWriterOutput string
		expectedError        string
	}{
		{
			name:                 "Test_Numeric_DefaultAnnotation",
			header:               "foo[numeric()]",
			input:                "12.2",
			expectedHeaderName:   "foo",
			expectedWriterOutput: "12.2::numeric as foo",
		},
		{
			name:                 "Test_Numeric_IntegerAnnotated",
			header:               "Bar[numeric(14,0)]",
			input:                "10",
			expectedHeaderName:   "Bar",
			expectedWriterOutput: "10::numeric(14,0) as Bar",
		},
		{
			name:                 "Test_Numeric_AnnotationCaseInsensitive",
			header:               "qUx[NumeriC(14,0)]",
			input:                "10",
			expectedHeaderName:   "qUx",
			expectedWriterOutput: "10::numeric(14,0) as qUx",
		},
		{
			name:          "Test_Numeric_Exception_AnnotatedPrecision_UnspecifiedScale",
			header:        "foo[numeric(10)]",
			input:         "12.2",
			expectedError: "scale must be spesified along with precision",
		},
		{
			name:          "Test_Numeric_Exception_UnspecifiedPrecision_AnnotatedScale",
			header:        "foo[numeric(,10)]",
			input:         "12.2",
			expectedError: "precision must be spesified along with scale",
		},
		{
			name:          "Test_Numeric_Exception_OutOfRangePrecision",
			header:        "foo[numeric(1001,10)]",
			input:         "12.2",
			expectedError: "invalid precision value: '1001', must be in range 0-1000",
		},
		{
			name:          "Test_Numeric_Exception_ScaleLargerOrEqualToPrecision",
			header:        "foo[numeric(1000,1000)]",
			input:         "12.2",
			expectedError: "invalid scale value: '1000', must be smaller than precision value '1000'",
		},
		{
			name:          "Test_Numeric_Exception_InvalidPrecision_ValidScale",
			header:        "foo[numeric(ten, 4)]",
			expectedError: "invalid precision value: 'ten'",
		},
		{
			name:          "Test_Numeric_Exception_ValidPrecision_InvalidScale",
			header:        "foo[numeric(10, four)]",
			expectedError: "invalid scale value: 'four'",
		},
		{
			name:          "Test_Numeric_Exception_InvalidPrecision_ValidScale",
			header:        "foo[numeric(ten, four)]",
			expectedError: "invalid precision value: 'ten'",
		},
		{
			name:          "Test_Numeric_Exception_InvalidNumeric",
			header:        "foo[numeric(4,0)]",
			input:         "not-a-number",
			expectedError: "error converting value 'not-a-number' to float",
		},
		{
			name:          "Test_Numeric_Exception_WrongType",
			header:        "foo[integer()]",
			expectedError: "invalid signature 'foo[integer()]'. Expected () or (<optional-precision>,<optional-scale>)",
		},
		{
			name:          "Test_Numeric_Exception_ExtraComma",
			header:        "foo[numeric(14,3,)]",
			expectedError: "invalid signature 'foo[numeric(14,3,)]'. Expected () or (<optional-precision>,<optional-scale>)",
		},
		{
			name:          "Test_Numeric_Exception_ExtraOpeningParenthesis",
			header:        "foo[numeric((14,3)]",
			expectedError: "unbalanced parentheses in signature 'foo[numeric((14,3)]'",
		},
		{
			name:          "Test_Numeric_Exception_ExtraClosingParenthesis",
			header:        "foo[numeric(14,3))]",
			expectedError: "unbalanced parentheses in signature 'foo[numeric(14,3))]'",
		},
		{
			name:          "Test_Numeric_Exception_MissingOpeningParenthesis",
			header:        "foo[numeric14,3)]",
			expectedError: "unbalanced parentheses in signature 'foo[numeric14,3)]'",
		},
		{
			name:          "Test_Numeric_Exception_MissingClosingParenthesis",
			header:        "foo[numeric(14,3]",
			expectedError: "unbalanced parentheses in signature 'foo[numeric(14,3]'",
		},
		{
			name:          "Test_Numeric_Exception_ExtraContentOutsideParenthesiss",
			header:        "foo[numeric(14,3)]ExtraContent",
			expectedError: "invalid signature 'foo[numeric(14,3)]ExtraContent'. Signature should be of the form <name>[numeric(<optional-precision>,<optional-scale>)]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := numeric.Numeric{}
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
