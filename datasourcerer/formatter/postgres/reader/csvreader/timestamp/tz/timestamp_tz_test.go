package tz_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/postgres/reader/csvreader/timestamp/tz"
)

func Test_Timestamp_Time_Zone(t *testing.T) {
	tests := []struct {
		name                 string
		header               string
		input                string
		expectedHeaderName   string
		expectedWriterOutput string
		expectedError        string
	}{
		{
			name:                 "Test_Timestamp_Time_Zone_DefaultAnnotation",
			header:               "foo[timestamp_tz()]",
			input:                "2000-12-31 23:59:59",
			expectedHeaderName:   "foo",
			expectedWriterOutput: "'2000-12-31 23:59:59'::timestamp(6) with time zone as foo",
		},
		{
			name:                 "Test_Timestamp_Time_Zone_CustomFormat",
			header:               "Bar[timestamp_tz(yyyy-MM-ddTHH:mm:ssZ)]",
			input:                "2000-12-31T23:59:59Z",
			expectedHeaderName:   "Bar",
			expectedWriterOutput: "'2000-12-31 23:59:59'::timestamp(6) with time zone as Bar",
		},
		{
			name:                 "Test_Timestamp_Time_Zone_CustomPrecision",
			header:               "qUx[timestamp_tz(,3)]",
			input:                "2000-12-31 23:59:59",
			expectedHeaderName:   "qUx",
			expectedWriterOutput: "'2000-12-31 23:59:59'::timestamp(3) with time zone as qUx",
		},
		{
			name:          "Test_Timestamp_Time_Zone_Excepton_OneExtraComma",
			header:        "foo[timestamp_tz(,,)]",
			expectedError: "invalid signature 'foo[timestamp_tz(,,)]'. Expected () or (<optional-format>,<optional-precision>)",
		},
		{
			name:          "Test_Timestamp_Time_Zone_Exception_InvalidValue",
			header:        "foo[timestamp_tz()]",
			input:         "not-a-timestamp",
			expectedError: "not able to convert value 'not-a-timestamp' to timestamp using the '2006-01-02 15:04:05' format",
		},
		{
			name:          "Test_Timestamp_Time_Zone_Excepton_InvalidPrecisionNonNumeric",
			header:        "foo[timestamp_tz(,abc)]",
			expectedError: "invalid precision 'abc' in signature 'foo[timestamp_tz(,abc)]'. Expected int 0-6",
		},
		{
			name:          "Test_Timestamp_Time_Zone_Excepton_InvalidPrecisionOutOfRange",
			header:        "foo[timestamp_tz(,10)]",
			expectedError: "precision must be between 0 and 6. Got '10'",
		},
		{
			name:          "Test_Timestamp_Time_Zone_Exception_ExtraOpeningParenthesis",
			header:        "foo[timestamp_tz(()]",
			expectedError: "unbalanced parentheses in signature 'foo[timestamp_tz(()]'",
		},
		{
			name:          "Test_Timestamp_Time_Zone_Exception_ExtraClosingParenthesis",
			header:        "foo[timestamp_tz())]",
			expectedError: "unbalanced parentheses in signature 'foo[timestamp_tz())]'",
		},
		{
			name:          "Test_Timestamp_Time_Zone_Exception_MissingOpeningParenthesis",
			header:        "foo[timestamp_tz)]",
			expectedError: "unbalanced parentheses in signature 'foo[timestamp_tz)]'",
		},
		{
			name:          "Test_Timestamp_Time_Zone_Exception_MissingClosingParenthesis",
			header:        "foo[timestamp_tz(]",
			expectedError: "unbalanced parentheses in signature 'foo[timestamp_tz(]'",
		},
		{
			name:          "Test_Timestamp_Time_Zone_Exception_ExtraContentOutsideParenthesiss",
			header:        "foo[timestamp_tz()]ExtraContent",
			expectedError: "invalid signature 'foo[timestamp_tz()]ExtraContent'. Signature should be of the form <name>[timestamp_tz(<optional-format>,<optional-precision>)]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := &tz.Timestamptz{}
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
