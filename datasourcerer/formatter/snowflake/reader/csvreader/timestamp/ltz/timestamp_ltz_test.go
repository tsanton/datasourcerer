package ltz_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/snowflake/reader/csvreader/timestamp/ltz"
)

func Test_Timestamp_Ltz(t *testing.T) {
	tests := []struct {
		name                 string
		header               string
		input                string
		expectedHeaderName   string
		expectedWriterOutput string
		expectedError        string
	}{
		{
			name:                 "Test_Timestamp_Ltz_DefaultAnnotation",
			header:               "foo[timestamp_ltz()]",
			input:                "2000-12-31 23:59:59",
			expectedHeaderName:   "FOO",
			expectedWriterOutput: "'2000-12-31 23:59:59'::TIMESTAMP_LTZ(9) AS FOO",
		},
		{
			name:                 "Test_Timestamp_Ltz_CustomFormat",
			header:               "foo[timestamp_ltz(yyyy-MM-ddTHH:mm:ssZ)]",
			input:                "2000-12-31T23:59:59Z",
			expectedHeaderName:   "FOO",
			expectedWriterOutput: "'2000-12-31 23:59:59'::TIMESTAMP_LTZ(9) AS FOO",
		},
		{
			name:                 "Test_Timestamp_Ltz_CustomPrecision",
			header:               "foo[timestamp_ltz(,3)]",
			input:                "2000-12-31 23:59:59",
			expectedHeaderName:   "FOO",
			expectedWriterOutput: "'2000-12-31 23:59:59'::TIMESTAMP_LTZ(3) AS FOO",
		},
		{
			name:                 "Test_Timestamp_Ltz_AnnotationCaseInsensitive",
			header:               "foo[Timestamp_LTZ()]",
			input:                "2000-12-31 23:59:59",
			expectedHeaderName:   "FOO",
			expectedWriterOutput: "'2000-12-31 23:59:59'::TIMESTAMP_LTZ(9) AS FOO",
		},
		{
			name:          "Test_Timestamp_Ltz_Excepton_OneExtraComma",
			header:        "foo[timestamp_ltz(,,)]",
			expectedError: "invalid signature 'foo[timestamp_ltz(,,)]'. Expected () or (<optional-format>,<optional-precision>)",
		},
		{
			name:          "Test_Timestamp_Ltz_Exception_InvalidValue",
			header:        "foo[timestamp_ltz()]",
			input:         "not-a-timestamp",
			expectedError: "not able to convert value 'not-a-timestamp' to timestamp using the '2006-01-02 15:04:05' format",
		},
		{
			name:          "Test_Timestamp_Ltz_Excepton_InvalidPrecisionNonNumeric",
			header:        "foo[timestamp_ltz(,abc)]",
			expectedError: "invalid precision 'abc' in signature 'foo[timestamp_ltz(,abc)]'. Expected int 0-9",
		},
		{
			name:          "Test_Timestamp_Ltz_Excepton_InvalidPrecisionOutOfRange",
			header:        "foo[timestamp_ltz(,10)]",
			expectedError: "precision must be between 0 and 9. Got '10'",
		},
		{
			name:          "Test_Timestamp_Ltz_Exception_ExtraOpeningParenthesis",
			header:        "foo[timestamp_ltz(()]",
			expectedError: "unbalanced parentheses in signature 'foo[timestamp_ltz(()]'",
		},
		{
			name:          "Test_Timestamp_Ltz_Exception_ExtraClosingParenthesis",
			header:        "foo[timestamp_ltz())]",
			expectedError: "unbalanced parentheses in signature 'foo[timestamp_ltz())]'",
		},
		{
			name:          "Test_Timestamp_Ltz_Exception_MissingOpeningParenthesis",
			header:        "foo[timestamp)]",
			expectedError: "unbalanced parentheses in signature 'foo[timestamp)]'",
		},
		{
			name:          "Test_Timestamp_Ltz_Exception_MissingClosingParenthesis",
			header:        "foo[timestamp_ltz(]",
			expectedError: "unbalanced parentheses in signature 'foo[timestamp_ltz(]'",
		},
		{
			name:          "Test_Timestamp_Ltz_Exception_ExtraContentOutsideParenthesiss",
			header:        "foo[timestamp_ltz()]ExtraContent",
			expectedError: "invalid signature 'foo[timestamp_ltz()]ExtraContent'. Signature should be of the form <name>[timestamp_ltz(<optional-format>,<optional-precision>)]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := &ltz.TimestampLtz{}
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
