package datetime_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/snowflake/reader/csvreader/timestamp/datetime"
)

func Test_Datetime(t *testing.T) {
	tests := []struct {
		name                 string
		header               string
		input                string
		expectedHeaderName   string
		expectedWriterOutput string
		expectedError        string
	}{
		{
			name:                 "Test_Datetime_DefaultAnnotation",
			header:               "foo[datetime()]",
			input:                "2000-12-31 23:59:59",
			expectedHeaderName:   "FOO",
			expectedWriterOutput: "'2000-12-31 23:59:59'::DATETIME(9) AS FOO",
		},
		{
			name:                 "Test_Datetime_CustomFormat",
			header:               "foo[datetime(yyyy-MM-ddTHH:mm:ssZ)]",
			input:                "2000-12-31T23:59:59Z",
			expectedHeaderName:   "FOO",
			expectedWriterOutput: "'2000-12-31 23:59:59'::DATETIME(9) AS FOO",
		},
		{
			name:                 "Test_Datetime_CustomPrecision",
			header:               "foo[datetime(,3)]",
			input:                "2000-12-31 23:59:59",
			expectedHeaderName:   "FOO",
			expectedWriterOutput: "'2000-12-31 23:59:59'::DATETIME(3) AS FOO",
		},
		{
			name:                 "Test_Datetime_AnnotationCaseInsensitive",
			header:               "foo[DateTime()]",
			input:                "2000-12-31 23:59:59",
			expectedHeaderName:   "FOO",
			expectedWriterOutput: "'2000-12-31 23:59:59'::DATETIME(9) AS FOO",
		},
		{
			name:          "Test_Datetime_Excepton_OneExtraComma",
			header:        "foo[datetime(,,)]",
			expectedError: "invalid signature 'foo[datetime(,,)]'. Expected () or (<optional-format>,<optional-precision>)",
		},
		{
			name:          "Test_Datetime_Exception_InvalidValue",
			header:        "foo[datetime()]",
			input:         "not-a-timestamp",
			expectedError: "not able to convert value 'not-a-timestamp' to timestamp using the '2006-01-02 15:04:05' format",
		},
		{
			name:          "Test_Datetime_Excepton_InvalidPrecisionNonNumeric",
			header:        "foo[datetime(,abc)]",
			expectedError: "invalid precision 'abc' in signature 'foo[datetime(,abc)]'. Expected int 0-9",
		},
		{
			name:          "Test_Datetime_Excepton_InvalidPrecisionOutOfRange",
			header:        "foo[datetime(,10)]",
			expectedError: "precision must be between 0 and 9. Got '10'",
		},
		{
			name:          "Test_Datetime_Exception_ExtraOpeningParenthesis",
			header:        "foo[datetime(()]",
			expectedError: "unbalanced parentheses in signature 'foo[datetime(()]'",
		},
		{
			name:          "Test_Datetime_Exception_ExtraClosingParenthesis",
			header:        "foo[datetime())]",
			expectedError: "unbalanced parentheses in signature 'foo[datetime())]'",
		},
		{
			name:          "Test_Datetime_Exception_MissingOpeningParenthesis",
			header:        "foo[timestamp)]",
			expectedError: "unbalanced parentheses in signature 'foo[timestamp)]'",
		},
		{
			name:          "Test_Datetime_Exception_MissingClosingParenthesis",
			header:        "foo[datetime(]",
			expectedError: "unbalanced parentheses in signature 'foo[datetime(]'",
		},
		{
			name:          "Test_Datetime_Exception_ExtraContentOutsideParenthesiss",
			header:        "foo[datetime()]ExtraContent",
			expectedError: "invalid signature 'foo[datetime()]ExtraContent'. Signature should be of the form <name>[datetime(<optional-format>,<optional-precision>)]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := &datetime.Datetime{}
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
