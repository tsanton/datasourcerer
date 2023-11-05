package time_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/snowflake/reader/csvreader/time"
)

func Test_Time(t *testing.T) {
	tests := []struct {
		name                 string
		header               string
		input                string
		expectedHeaderName   string
		expectedWriterOutput string
		expectedError        string
	}{
		{
			name:                 "Test_Time_DefaultAnnotation",
			header:               "foo[time()]",
			input:                "23:59:59",
			expectedHeaderName:   "FOO",
			expectedWriterOutput: "'23:59:59'::TIME(9) AS FOO",
		},
		{
			name:                 "Test_Time_AnnotationCaseInsensitive",
			header:               "Bar[TimE()]",
			input:                "23:59:59",
			expectedHeaderName:   "BAR",
			expectedWriterOutput: "'23:59:59'::TIME(9) AS BAR",
		},
		{
			name:                 "Test_Time_CustomFormat",
			header:               "foo[time(HH:mm:ss.SSS)]",
			input:                "23:59:59.123",
			expectedHeaderName:   "FOO",
			expectedWriterOutput: "'23:59:59'::TIME(9) AS FOO",
		},
		{
			name:                 "Test_Time_CustomPrecision",
			header:               "foo[time(,3)]",
			input:                "23:59:59",
			expectedHeaderName:   "FOO",
			expectedWriterOutput: "'23:59:59'::TIME(3) AS FOO",
		},
		{
			name:          "Test_Time_Exception_InvalidValue",
			header:        "foo[time(,3)]",
			input:         "not-a-time",
			expectedError: "not able to convert value 'not-a-time' to time using the '15:04:05' format",
		},
		{
			name:          "Test_Time_Excepton_OneExtraComma",
			header:        "foo[time(,,)]",
			expectedError: "invalid signature 'foo[time(,,)]'. Expected () or (<optional-format>,<optional-precision>)",
		},
		{
			name:          "Test_Time_Excepton_InvalidPrecisionNonNumeric",
			header:        "foo[time(,abc)]",
			expectedError: "invalid precision 'abc' in signature 'foo[time(,abc)]'. Expected int 0-9",
		},
		{
			name:          "Test_Time_Excepton_InvalidPrecisionOutOfRange",
			header:        "foo[time(,10)]",
			expectedError: "precision must be between 0 and 9. Got '10'",
		},
		{
			name:          "Test_Time_Exception_ExtraOpeningParenthesis",
			header:        "foo[time(()]",
			expectedError: "unbalanced parentheses in signature 'foo[time(()]'",
		},
		{
			name:          "Test_Time_Exception_ExtraClosingParenthesis",
			header:        "foo[time())]",
			expectedError: "unbalanced parentheses in signature 'foo[time())]'",
		},
		{
			name:          "Test_Time_Exception_MissingOpeningParenthesis",
			header:        "foo[time)]",
			expectedError: "unbalanced parentheses in signature 'foo[time)]'",
		},
		{
			name:          "Test_Time_Exception_MissingClosingParenthesis",
			header:        "foo[time(]",
			expectedError: "unbalanced parentheses in signature 'foo[time(]'",
		},
		{
			name:          "Test_Time_Exception_ExtraContentOutsideParenthesiss",
			header:        "foo[time()]ExtraContent",
			expectedError: "invalid signature 'foo[time()]ExtraContent'. Signature should be of the form <name>[time(<optional-format>,<optional-precision>)]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := &time.Time{}
			err := header.ParseHeader(tt.header)

			if tt.expectedError != "" {
				if tt.input != "" && err == nil {
					content, err := header.GetWriter()(tt.input)
					assert.NotNil(t, err)
					assert.EqualError(t, err, tt.expectedError)
					assert.Nil(t, content)
				} else {
					assert.Error(t, err)
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
