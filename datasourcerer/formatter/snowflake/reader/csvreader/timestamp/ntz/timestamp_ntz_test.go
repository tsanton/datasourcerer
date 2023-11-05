package ntz_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/snowflake/reader/csvreader/timestamp/ntz"
)

func Test_Timestamp_Ntz(t *testing.T) {
	tests := []struct {
		name                 string
		header               string
		input                string
		expectedHeaderName   string
		expectedWriterOutput string
		expectedError        string
	}{
		{
			name:                 "Test_Timestamp_Ntz_DefaultAnnotation",
			header:               "foo[timestamp_ntz()]",
			input:                "2000-12-31 23:59:59",
			expectedHeaderName:   "FOO",
			expectedWriterOutput: "'2000-12-31 23:59:59'::TIMESTAMP_NTZ(9) AS FOO",
		},
		{
			name:                 "Test_Timestamp_Ntz_CustomFormat",
			header:               "foo[timestamp_ntz(yyyy-MM-ddTHH:mm:ssZ)]",
			input:                "2000-12-31T23:59:59Z",
			expectedHeaderName:   "FOO",
			expectedWriterOutput: "'2000-12-31 23:59:59'::TIMESTAMP_NTZ(9) AS FOO",
		},
		{
			name:                 "Test_Timestamp_Ntz_CustomPrecision",
			header:               "foo[timestamp_ntz(,3)]",
			input:                "2000-12-31 23:59:59",
			expectedHeaderName:   "FOO",
			expectedWriterOutput: "'2000-12-31 23:59:59'::TIMESTAMP_NTZ(3) AS FOO",
		},
		{
			name:                 "Test_Timestamp_Ntz_AnnotationCaseInsensitive",
			header:               "foo[Timestamp_NTZ()]",
			input:                "2000-12-31 23:59:59",
			expectedHeaderName:   "FOO",
			expectedWriterOutput: "'2000-12-31 23:59:59'::TIMESTAMP_NTZ(9) AS FOO",
		},
		{
			name:          "Test_Timestamp_Ntz_Excepton_OneExtraComma",
			header:        "foo[timestamp_ntz(,,)]",
			expectedError: "invalid signature 'foo[timestamp_ntz(,,)]'. Expected () or (<optional-format>,<optional-precision>)",
		},
		{
			name:          "Test_Timestamp_Ntz_Exception_InvalidValue",
			header:        "foo[timestamp_ntz()]",
			input:         "not-a-timestamp",
			expectedError: "not able to convert value 'not-a-timestamp' to timestamp using the '2006-01-02 15:04:05' format",
		},
		{
			name:          "Test_Timestamp_Ntz_Excepton_InvalidPrecisionNonNumeric",
			header:        "foo[timestamp_ntz(,abc)]",
			expectedError: "invalid precision 'abc' in signature 'foo[timestamp_ntz(,abc)]'. Expected int 0-9",
		},
		{
			name:          "Test_Timestamp_Ntz_Excepton_InvalidPrecisionOutOfRange",
			header:        "foo[timestamp_ntz(,10)]",
			expectedError: "precision must be between 0 and 9. Got '10'",
		},
		{
			name:          "Test_Timestamp_Ntz_Exception_ExtraOpeningParenthesis",
			header:        "foo[timestamp_ntz(()]",
			expectedError: "unbalanced parentheses in signature 'foo[timestamp_ntz(()]'",
		},
		{
			name:          "Test_Timestamp_Ntz_Exception_ExtraClosingParenthesis",
			header:        "foo[timestamp_ntz())]",
			expectedError: "unbalanced parentheses in signature 'foo[timestamp_ntz())]'",
		},
		{
			name:          "Test_Timestamp_Ntz_Exception_MissingOpeningParenthesis",
			header:        "foo[timestamp)]",
			expectedError: "unbalanced parentheses in signature 'foo[timestamp)]'",
		},
		{
			name:          "Test_Timestamp_Ntz_Exception_MissingClosingParenthesis",
			header:        "foo[timestamp_ntz(]",
			expectedError: "unbalanced parentheses in signature 'foo[timestamp_ntz(]'",
		},
		{
			name:          "Test_Timestamp_Ntz_Exception_ExtraContentOutsideParenthesiss",
			header:        "foo[timestamp_ntz()]ExtraContent",
			expectedError: "invalid signature 'foo[timestamp_ntz()]ExtraContent'. Signature should be of the form <name>[timestamp_ntz(<optional-format>,<optional-precision>)]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := &ntz.TimestampNtz{}
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
