package ntz_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/postgres/reader/csvreader/timestamp/ntz"
)

func Test_Timestamp_No_Time_Zone_No_Time_Zone(t *testing.T) {
	tests := []struct {
		name                 string
		header               string
		input                string
		expectedHeaderName   string
		expectedWriterOutput string
		expectedError        string
	}{
		{
			name:                 "Test_Timestamp_No_Time_Zone_DefaultAnnotation",
			header:               "foo[timestamp()]",
			input:                "2000-12-31 23:59:59",
			expectedHeaderName:   "foo",
			expectedWriterOutput: "'2000-12-31 23:59:59'::timestamp(6) as foo",
		},
		{
			name:                 "Test_Timestamp_No_Time_Zone_AnnotationCaseInsensitive",
			header:               "XxX[TimestamP()]",
			input:                "2000-12-31 23:59:59",
			expectedHeaderName:   "XxX",
			expectedWriterOutput: "'2000-12-31 23:59:59'::timestamp(6) as XxX",
		},
		{
			name:                 "Test_Timestamp_No_Time_Zone_CustomFormat",
			header:               "Bar[timestamp(yyyy-MM-ddTHH:mm:ssZ)]",
			input:                "2000-12-31T23:59:59Z",
			expectedHeaderName:   "Bar",
			expectedWriterOutput: "'2000-12-31 23:59:59'::timestamp(6) as Bar",
		},
		{
			name:                 "Test_Timestamp_No_Time_Zone_CustomPrecision",
			header:               "qUx[timestamp(,3)]",
			input:                "2000-12-31 23:59:59",
			expectedHeaderName:   "qUx",
			expectedWriterOutput: "'2000-12-31 23:59:59'::timestamp(3) as qUx",
		},
		{
			name:          "Test_Timestamp_No_Time_Zone_Excepton_OneExtraComma",
			header:        "foo[timestamp(,,)]",
			expectedError: "invalid signature 'foo[timestamp(,,)]'. Expected () or (<optional-format>,<optional-precision>)",
		},
		{
			name:          "Test_Timestamp_No_Time_Zone_Exception_InvalidValue",
			header:        "foo[timestamp()]",
			input:         "not-a-timestamp",
			expectedError: "not able to convert value 'not-a-timestamp' to timestamp using the '2006-01-02 15:04:05' format",
		},
		{
			name:          "Test_Timestamp_No_Time_Zone_Excepton_InvalidPrecisionNonNumeric",
			header:        "foo[timestamp(,abc)]",
			expectedError: "invalid precision 'abc' in signature 'foo[timestamp(,abc)]'. Expected int 0-6",
		},
		{
			name:          "Test_Timestamp_No_Time_Zone_Excepton_InvalidPrecisionOutOfRange",
			header:        "foo[timestamp(,10)]",
			expectedError: "precision must be between 0 and 6. Got '10'",
		},
		{
			name:          "Test_Timestamp_No_Time_Zone_Exception_ExtraOpeningParenthesis",
			header:        "foo[timestamp(()]",
			expectedError: "unbalanced parentheses in signature 'foo[timestamp(()]'",
		},
		{
			name:          "Test_Timestamp_No_Time_Zone_Exception_ExtraClosingParenthesis",
			header:        "foo[timestamp())]",
			expectedError: "unbalanced parentheses in signature 'foo[timestamp())]'",
		},
		{
			name:          "Test_Timestamp_No_Time_Zone_Exception_MissingOpeningParenthesis",
			header:        "foo[timestamp)]",
			expectedError: "unbalanced parentheses in signature 'foo[timestamp)]'",
		},
		{
			name:          "Test_Timestamp_No_Time_Zone_Exception_MissingClosingParenthesis",
			header:        "foo[timestamp(]",
			expectedError: "unbalanced parentheses in signature 'foo[timestamp(]'",
		},
		{
			name:          "Test_Timestamp_No_Time_Zone_Exception_ExtraContentOutsideParenthesiss",
			header:        "foo[timestamp()]ExtraContent",
			expectedError: "invalid signature 'foo[timestamp()]ExtraContent'. Signature should be of the form <name>[timestamp(<optional-format>,<optional-precision>)]",
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
