package tz_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/snowflake/reader/csvreader/timestamp/tz"
)

func Test_Timestamp_Tz(t *testing.T) {
	tests := []struct {
		name                 string
		header               string
		input                string
		expectedHeaderName   string
		expectedWriterOutput string
		expectedError        string
	}{
		{
			name:                 "Test_Timestamp_Tz_DefaultAnnotation",
			header:               "foo[timestamp_tz()]",
			input:                "2000-12-31 23:59:59",
			expectedHeaderName:   "FOO",
			expectedWriterOutput: "'2000-12-31 23:59:59'::TIMESTAMP_TZ(9) AS FOO",
		},
		{
			name:                 "Test_Timestamp_Tz_CustomFormat",
			header:               "foo[timestamp_tz(yyyy-MM-ddTHH:mm:ssZ)]",
			input:                "2000-12-31T23:59:59Z",
			expectedHeaderName:   "FOO",
			expectedWriterOutput: "'2000-12-31 23:59:59'::TIMESTAMP_TZ(9) AS FOO",
		},
		{
			name:                 "Test_Timestamp_Tz_CustomPrecision",
			header:               "foo[timestamp_tz(,3)]",
			input:                "2000-12-31 23:59:59",
			expectedHeaderName:   "FOO",
			expectedWriterOutput: "'2000-12-31 23:59:59'::TIMESTAMP_TZ(3) AS FOO",
		},
		{
			name:                 "Test_Timestamp_Tz_AnnotationCaseInsensitive",
			header:               "foo[timestamp_tz()]",
			input:                "2000-12-31 23:59:59",
			expectedHeaderName:   "FOO",
			expectedWriterOutput: "'2000-12-31 23:59:59'::TIMESTAMP_TZ(9) AS FOO",
		},
		{
			name:          "Test_Timestamp_Tz_Excepton_OneExtraComma",
			header:        "foo[timestamp_tz(,,)]",
			expectedError: "invalid signature 'foo[timestamp_tz(,,)]'. Expected () or (<optional-format>,<optional-precision>)",
		},
		{
			name:          "Test_Timestamp_Tz_Exception_InvalidValue",
			header:        "foo[timestamp_tz()]",
			input:         "not-a-timestamp",
			expectedError: "not able to convert value 'not-a-timestamp' to timestamp using the '2006-01-02 15:04:05' format",
		},
		{
			name:          "Test_Timestamp_Tz_Excepton_InvalidPrecisionNonNumeric",
			header:        "foo[timestamp_tz(,abc)]",
			expectedError: "invalid precision 'abc' in signature 'foo[timestamp_tz(,abc)]'. Expected int 0-9",
		},
		{
			name:          "Test_Timestamp_Tz_Excepton_InvalidPrecisionOutOfRange",
			header:        "foo[timestamp_tz(,10)]",
			expectedError: "precision must be between 0 and 9. Got '10'",
		},
		{
			name:          "Test_Timestamp_Tz_Exception_ExtraOpeningParenthesis",
			header:        "foo[timestamp_tz(()]",
			expectedError: "unbalanced parentheses in signature 'foo[timestamp_tz(()]'",
		},
		{
			name:          "Test_Timestamp_Tz_Exception_ExtraClosingParenthesis",
			header:        "foo[timestamp_tz())]",
			expectedError: "unbalanced parentheses in signature 'foo[timestamp_tz())]'",
		},
		{
			name:          "Test_Timestamp_Tz_Exception_MissingOpeningParenthesis",
			header:        "foo[timestamp)]",
			expectedError: "unbalanced parentheses in signature 'foo[timestamp)]'",
		},
		{
			name:          "Test_Timestamp_Tz_Exception_MissingClosingParenthesis",
			header:        "foo[timestamp_tz(]",
			expectedError: "unbalanced parentheses in signature 'foo[timestamp_tz(]'",
		},
		{
			name:          "Test_Timestamp_Tz_Exception_ExtraContentOutsideParenthesiss",
			header:        "foo[timestamp_tz()]ExtraContent",
			expectedError: "invalid signature 'foo[timestamp_tz()]ExtraContent'. Signature should be of the form <name>[timestamp_tz(<optional-format>,<optional-precision>)]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := &tz.TimestampTz{}
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
