package date_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter/postgres/reader/csvreader/date"
)

func Test_Date(t *testing.T) {
	tests := []struct {
		name                 string
		header               string
		input                string
		expectedHeaderName   string
		expectedWriterOutput string
		expectedError        string
	}{
		{
			name:                 "Test_Date_DefaultAnnotation",
			header:               "foo[date()]",
			input:                "2000-12-31",
			expectedHeaderName:   "foo",
			expectedWriterOutput: "'2000-12-31'::date as foo",
		},
		{
			name:                 "Test_Date_AnnotationCaseInsensitive",
			header:               "Bar[DaTe()]",
			input:                "2000-12-31",
			expectedHeaderName:   "Bar",
			expectedWriterOutput: "'2000-12-31'::date as Bar",
		},
		{
			name:                 "Test_Date_AnnotatedNormal",
			header:               "BaZ[date(yyyy/MM/dd)]",
			input:                "2000/12/31",
			expectedHeaderName:   "BaZ",
			expectedWriterOutput: "'2000-12-31'::date as BaZ",
		},
		{
			name:                 "Test_Date_AnnotatedGo",
			header:               "qUx[date(2006/01/02)]",
			input:                "2000/12/31",
			expectedHeaderName:   "qUx",
			expectedWriterOutput: "'2000-12-31'::date as qUx",
		},
		{
			name:          "Test_Date_Exception_InvalidValue",
			header:        "foo[date(yyyy-MM-dd)]",
			input:         "not-a-date",
			expectedError: "not able to convert value 'not-a-date' to date using the '2006-01-02' format",
		},
		{
			name:          "Test_Date_Exception_MalformedDateFormat",
			header:        "foo[date(aaaa-MM-dd)]",
			input:         "2000/12/31",
			expectedError: "not able to convert value '2000/12/31' to date using the 'aaaa-MM-dd' format",
		},
		{
			name:          "Test_Date_Exception_ExtraOpeningParenthesis",
			header:        "foo[date((yyyy-MM-dd)]",
			expectedError: "unbalanced parentheses in signature 'foo[date((yyyy-MM-dd)]'",
		},
		{
			name:          "Test_Date_Exception_ExtraClosingParenthesis",
			header:        "foo[date(yyyy-MM-dd))]",
			expectedError: "unbalanced parentheses in signature 'foo[date(yyyy-MM-dd))]'",
		},
		{
			name:          "Test_Date_Exception_MissingClosingParenthesis",
			header:        "foo[date(yyyy-MM-dd]",
			expectedError: "unbalanced parentheses in signature 'foo[date(yyyy-MM-dd]'",
		},
		{
			name:          "Test_Date_Exception_WrongType",
			header:        "foo[integer(yyyy-MM-dd)]",
			expectedError: "invalid signature 'foo[integer(yyyy-MM-dd)]'. Expected () or (<format>)",
		},
		{
			name:          "Test_Date_Exception_ExtraContentOutsideParenthesis",
			header:        "foo[date(yyyy-MM-dd)]ExtraContent",
			expectedError: "invalid signature 'foo[date(yyyy-MM-dd)]ExtraContent'. Signature should be of the form <name>[date(<format>)]",
		},
		{
			name:          "Test_Date_Exception_ExtraComma",
			header:        "foo[date(yyyy-MM-dd,)]",
			expectedError: "invalid signature 'foo[date(yyyy-MM-dd,)]'. Expected () or (<format>)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := date.Date{}
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
