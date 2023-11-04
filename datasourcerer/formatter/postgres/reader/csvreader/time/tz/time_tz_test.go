package tz_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/postgres/reader/csvreader/time/tz"
)

func Test_Time_Tz(t *testing.T) {
	tests := []struct {
		name                 string
		header               string
		input                string
		expectedHeaderName   string
		expectedWriterOutput string
		expectedError        string
	}{
		{
			name:                 "Test_Time_Tz_DefaultAnnotation",
			header:               "foo[time_tz()]",
			input:                "23:59:59",
			expectedHeaderName:   "foo",
			expectedWriterOutput: "'23:59:59'::time(6) with time zone as foo",
		},
		{
			name:                 "Test_Time_Tz_AnnotationCaseInsensitive",
			header:               "foo[time_tz()]",
			input:                "23:59:59",
			expectedHeaderName:   "foo",
			expectedWriterOutput: "'23:59:59'::time(6) with time zone as foo",
		},
		{
			name:                 "Test_Time_Tz_CustomFormat",
			header:               "Bar[time_tz(HH:mm:ss.SSS)]",
			input:                "23:59:59.123",
			expectedHeaderName:   "Bar",
			expectedWriterOutput: "'23:59:59'::time(6) with time zone as Bar",
		},
		{
			name:                 "Test_Time_Tz_CustomPrecision",
			header:               "bAz[time_tz(,3)]",
			input:                "23:59:59",
			expectedHeaderName:   "bAz",
			expectedWriterOutput: "'23:59:59'::time(3) with time zone as bAz",
		},
		{
			name:          "Test_Time_Tz_Exception_InvalidValue",
			header:        "foo[time_tz(,3)]",
			input:         "not-a-time",
			expectedError: "not able to convert value 'not-a-time' to time using the '15:04:05' format",
		},
		{
			name:          "Test_Time_Tz_Excepton_OneExtraComma",
			header:        "foo[time_tz(,,)]",
			expectedError: "invalid signature 'foo[time_tz(,,)]'. Expected () or (<optional-format>,<optional-precision>)",
		},
		{
			name:          "Test_Time_Tz_Excepton_InvalidPrecisionNonNumeric",
			header:        "foo[time_tz(,abc)]",
			expectedError: "invalid precision 'abc' in signature 'foo[time_tz(,abc)]'. Expected int 0-6",
		},
		{
			name:          "Test_Time_Tz_Excepton_InvalidPrecisionOutOfRange",
			header:        "foo[time_tz(,7)]",
			expectedError: "precision must be between 0 and 6. Got '7'",
		},
		{
			name:          "Test_Time_Tz_Exception_ExtraOpeningParenthesis",
			header:        "foo[time_tz(()]",
			expectedError: "unbalanced parentheses in signature 'foo[time_tz(()]'",
		},
		{
			name:          "Test_Time_Tz_Exception_ExtraClosingParenthesis",
			header:        "foo[time_tz())]",
			expectedError: "unbalanced parentheses in signature 'foo[time_tz())]'",
		},
		{
			name:          "Test_Time_Tz_Exception_MissingOpeningParenthesis",
			header:        "foo[time_tz)]",
			expectedError: "unbalanced parentheses in signature 'foo[time_tz)]'",
		},
		{
			name:          "Test_Time_Tz_Exception_MissingClosingParenthesis",
			header:        "foo[time_tz(]",
			expectedError: "unbalanced parentheses in signature 'foo[time_tz(]'",
		},
		{
			name:          "Test_Time_Tz_Exception_ExtraContentOutsideParenthesiss",
			header:        "foo[time_tz()]ExtraContent",
			expectedError: "invalid signature 'foo[time_tz()]ExtraContent'. Signature should be of the form <name>[time_tz(<optional-format>,<optional-precision>)]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := &tz.Timetz{}
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
