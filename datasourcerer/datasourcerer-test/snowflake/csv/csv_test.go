package unit_test

import (
	"fmt"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/sirkon/go-format"
	"github.com/stretchr/testify/assert"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter"
	"github.com/tsanton/dbt-unit-test-fusionizer/testutils"
)

func Test_Snowflake_Csv_SingleDataSource(t *testing.T) {
	ds, out := testutils.BootstrapDirs()
	defer testutils.CleanupDir(ds, out)

	dataContent := strings.TrimSpace(`
"Id[number(10,0)]",Name,Address[varchar(40)],"Income[number(20,2)]",Single[boolean()],DateOfBirth[date()],CreatedAt[datetime()],CreatedTime[time()]
1,John,123 Main St,100.1,true,1990-01-15,2000-12-31 23:59:59,23:59:59
2,Jane,456 Main St,200.2,false,1985-12-25,1990-01-01 00:00:00,00:00:00
3,Kane,901 Main St,300.3,true,1975-07-07,1980-06-15 12:30:45,12:30:45
	`)
	dataSourceFile, err := testutils.CreateFile(ds.D1, "datasource.csv", dataContent, map[string]interface{}{})
	if err != nil {
		t.Fatalf("Error creating data source file: %s", err)
	}

	testContent := strings.TrimSpace(`
{{ config(tags=['unit-test']) }}

{% call dbt_unit_testing.test('<model-name>', '<test-name>') %}

	{% call dbt_unit_testing.mock_ref ('<source-name>', {'source_file': '${0}' }) %}
	{% endcall %}

	{% call dbt_unit_testing.expect() %}
select 'Gunnar' as name
	{% endcall %}

{% endcall %}
`)

	testFile, err := testutils.CreateFile(ds.D1, "test_snowflake.sql", testContent, format.Values{"0": dataSourceFile.Name()})
	if err != nil {
		t.Fatalf("Error creating test file: %s", err)
	}

	testutils.Run(logger, &formatter.Config{
		Filetype: formatter.ParserInputTypeCsv,
		CSV:      formatter.NewDefaultCsvConfig(),
	}, ds.RootDir, out.RootDir)

	expectedConversion := strings.TrimSpace(`
SELECT 1::NUMBER(10,0) AS ID, 'John'::VARCHAR(16777216) AS NAME, '123 Main St'::VARCHAR(40) AS ADDRESS, 100.1::NUMBER(20,2) AS INCOME, true::BOOLEAN AS SINGLE, '1990-01-15'::DATE AS DATEOFBIRTH, '2000-12-31 23:59:59'::DATETIME(9) AS CREATEDAT, '23:59:59'::TIME(9) AS CREATEDTIME
UNION ALL
SELECT 2::NUMBER(10,0) AS ID, 'Jane'::VARCHAR(16777216) AS NAME, '456 Main St'::VARCHAR(40) AS ADDRESS, 200.2::NUMBER(20,2) AS INCOME, false::BOOLEAN AS SINGLE, '1985-12-25'::DATE AS DATEOFBIRTH, '1990-01-01 00:00:00'::DATETIME(9) AS CREATEDAT, '00:00:00'::TIME(9) AS CREATEDTIME
UNION ALL
SELECT 3::NUMBER(10,0) AS ID, 'Kane'::VARCHAR(16777216) AS NAME, '901 Main St'::VARCHAR(40) AS ADDRESS, 300.3::NUMBER(20,2) AS INCOME, true::BOOLEAN AS SINGLE, '1975-07-07'::DATE AS DATEOFBIRTH, '1980-06-15 12:30:45'::DATETIME(9) AS CREATEDAT, '12:30:45'::TIME(9) AS CREATEDTIME
	`)
	m1 := testutils.MergeOptions{
		LineNumber: 4,
		Regex:      nil,
		Content:    expectedConversion,
	}

	expected := testutils.Merge(t, testContent, format.Values{"0": dataSourceFile.Name()}, m1)

	result, err := testutils.GetGeneratorFile(out.RootDir, ds.RootDir, testFile.Name())
	// fmt.Println(result)
	// fmt.Println(expected)
	assert.Nil(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, expected, result)
}

func Test_Snowflake_Csv_SingleDataSource_OnelineEndcall(t *testing.T) {
	ds, out := testutils.BootstrapDirs()
	defer testutils.CleanupDir(ds, out)

	dataContent := strings.TrimSpace(`
"Id[number(10,0)]",Name,Address[varchar(40)],"Income[number(20,2)]",Single[boolean()],DateOfBirth[date()],CreatedAt[datetime()],CreatedTime[time()]
1,John,123 Main St,100.1,true,1990-01-15,2000-12-31 23:59:59,23:59:59
2,Jane,456 Main St,200.2,false,1985-12-25,1990-01-01 00:00:00,00:00:00
3,Kane,901 Main St,300.3,true,1975-07-07,1980-06-15 12:30:45,12:30:45
	`)
	dataSourceFile, err := testutils.CreateFile(ds.D1, "datasource.csv", dataContent, map[string]interface{}{})
	if err != nil {
		t.Fatalf("Error creating data source file: %s", err)
	}

	testContent := strings.TrimSpace(`
{{ config(tags=['unit-test']) }}

{% call dbt_unit_testing.test('<model-name>', '<test-name>') %}

	{% call dbt_unit_testing.mock_ref ('<source-name>', {'source_file': '${0}' }) %}{% endcall %}

	{% call dbt_unit_testing.expect() %}
select 'Gunnar' as name
	{% endcall %}

{% endcall %}
`)

	testFile, err := testutils.CreateFile(ds.D1, "test_snowflake.sql", testContent, format.Values{"0": dataSourceFile.Name()})
	if err != nil {
		t.Fatalf("Error creating test file: %s", err)
	}

	testutils.Run(logger, &formatter.Config{
		Filetype: formatter.ParserInputTypeCsv,
		CSV:      formatter.NewDefaultCsvConfig(),
	}, ds.RootDir, out.RootDir)

	expectedConversion := strings.TrimSpace(`
SELECT 1::NUMBER(10,0) AS ID, 'John'::VARCHAR(16777216) AS NAME, '123 Main St'::VARCHAR(40) AS ADDRESS, 100.1::NUMBER(20,2) AS INCOME, true::BOOLEAN AS SINGLE, '1990-01-15'::DATE AS DATEOFBIRTH, '2000-12-31 23:59:59'::DATETIME(9) AS CREATEDAT, '23:59:59'::TIME(9) AS CREATEDTIME
UNION ALL
SELECT 2::NUMBER(10,0) AS ID, 'Jane'::VARCHAR(16777216) AS NAME, '456 Main St'::VARCHAR(40) AS ADDRESS, 200.2::NUMBER(20,2) AS INCOME, false::BOOLEAN AS SINGLE, '1985-12-25'::DATE AS DATEOFBIRTH, '1990-01-01 00:00:00'::DATETIME(9) AS CREATEDAT, '00:00:00'::TIME(9) AS CREATEDTIME
UNION ALL
SELECT 3::NUMBER(10,0) AS ID, 'Kane'::VARCHAR(16777216) AS NAME, '901 Main St'::VARCHAR(40) AS ADDRESS, 300.3::NUMBER(20,2) AS INCOME, true::BOOLEAN AS SINGLE, '1975-07-07'::DATE AS DATEOFBIRTH, '1980-06-15 12:30:45'::DATETIME(9) AS CREATEDAT, '12:30:45'::TIME(9) AS CREATEDTIME
`)
	m1 := testutils.MergeOptions{
		LineNumber: 4,
		Regex:      regexp.MustCompile(regexp.QuoteMeta("{'source_file': '${0}' }) %}")),
		Content:    expectedConversion,
	}

	expected := testutils.Merge(t, testContent, format.Values{"0": dataSourceFile.Name()}, m1)

	result, err := testutils.GetGeneratorFile(out.RootDir, ds.RootDir, testFile.Name())
	// fmt.Println(result)
	// fmt.Println(expected)
	assert.Nil(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, expected, result)
}

func Test_Snowflake_Csv_SingleDataSource_Malformed(t *testing.T) {
	ds, out := testutils.BootstrapDirs()
	defer testutils.CleanupDir(ds, out)

	dataContent := strings.TrimSpace(`
"Id[number(10,0)]",Name,"Income[number(20,2)]"
1,John,100.1
2,Jane,<MALFORMED>
3,Kane,300.3
	`)
	dataSourceFile, err := testutils.CreateFile(ds.D1, "datasource.csv", dataContent, map[string]interface{}{})
	if err != nil {
		t.Fatalf("Error creating data source file: %s", err)
	}

	testContent := strings.TrimSpace(`
{{ config(tags=['unit-test']) }}

{% call dbt_unit_testing.test('<model-name>', '<test-name>') %}

	{% call dbt_unit_testing.mock_ref ('<source-name>', {'source_file': '${0}' }) %}
	{% endcall %}

	{% call dbt_unit_testing.expect() %}
select 'Gunnar' as name
	{% endcall %}

{% endcall %}
`)

	testFile, err := testutils.CreateFile(ds.D1, "test_snowflake.sql", testContent, format.Values{"0": dataSourceFile.Name()})
	if err != nil {
		t.Fatalf("Error creating test file: %s", err)
	}

	testutils.Run(logger, &formatter.Config{
		Filetype: formatter.ParserInputTypeCsv,
		CSV:      formatter.NewDefaultCsvConfig(),
	}, ds.RootDir, out.RootDir)

	expectedConversion := strings.TrimSpace(`
	error parsing value '<MALFORMED>' for column 'INCOME' in line 3
	`)
	m1 := testutils.MergeOptions{
		LineNumber: 4,
		Regex:      nil,
		Content:    expectedConversion,
	}

	expected := testutils.Merge(t, testContent, format.Values{"0": dataSourceFile.Name()}, m1)

	result, err := testutils.GetGeneratorFile(out.RootDir, ds.RootDir, testFile.Name())
	fmt.Println(result)
	fmt.Println(expected)
	assert.Nil(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, expected, result)
}

func Test_Snowflake_Csv_SinglDataSource_DoesNotExist(t *testing.T) {
	ds, out := testutils.BootstrapDirs()
	defer testutils.CleanupDir(ds, out)

	testContent := strings.TrimSpace(`
{{ config(tags=['unit-test']) }}

{% call dbt_unit_testing.test('<model-name>', '<test-name>') %}

	{% call dbt_unit_testing.mock_ref ('<source-name>', {'source_file': '${0}' }) %}
	{% endcall %}

	{% call dbt_unit_testing.expect() %}
select 'Gunnar' as name
	{% endcall %}

{% endcall %}
`)

	testFile, err := testutils.CreateFile(ds.D1, "test_snowflake.sql", testContent, format.Values{"0": "./does_not_exist.csv"})
	if err != nil {
		t.Fatalf("Error creating test file: %s", err)
	}
	// testFilePath, err := filepath.Abs(testFile.Name())
	assert.Nil(t, err)

	testutils.Run(logger, &formatter.Config{
		Filetype: formatter.ParserInputTypeCsv,
		CSV:      formatter.NewDefaultCsvConfig(),
	}, ds.RootDir, out.RootDir)

	expectedConversion := strings.TrimSpace(`
error reading file '${1}': file not found
	`)
	m1 := testutils.MergeOptions{
		LineNumber: 4,
		Regex:      nil,
		Content:    expectedConversion,
	}
	//
	expected := testutils.Merge(t, testContent, format.Values{
		"0": "./does_not_exist.csv",
		"1": path.Join(filepath.Dir(testFile.Name()), "./does_not_exist.csv"),
	}, m1)
	result, err := testutils.GetGeneratorFile(out.RootDir, ds.RootDir, testFile.Name())
	// fmt.Println(result)
	// fmt.Println(expected)
	assert.Nil(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, expected, result)
}
