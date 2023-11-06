package unit_test

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/sirkon/go-format"
	"github.com/stretchr/testify/assert"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter"
	"github.com/tsanton/dbt-unit-test-fusionizer/testutils"
)

func Test_Snowflake_Sql_SingleTest_NoDataSources(t *testing.T) {
	ds, out := testutils.BootstrapDirs()
	defer testutils.CleanupDir(ds, out)

	testContent := strings.TrimSpace(`
{{ config(tags=['unit-test']) }}

{% call dbt_unit_testing.test('<model-name>', '<test-name>') %}

	{% call dbt_unit_testing.mock_ref ('<source-name>') %}
select 'Gunnar' as name
	{% endcall %}

	{% call dbt_unit_testing.expect() %}
select 'Gunnar' as name
	{% endcall %}

{% endcall %}
`)

	testFile, err := testutils.CreateFile(ds.D1, "test_snowflake.sql", testContent, map[string]interface{}{})
	if err != nil {
		t.Fatalf("Error creating test file: %s", err)
	}

	testutils.Run(logger, &formatter.Config{Filetype: formatter.ParserInputTypeSql}, ds.RootDir, out.RootDir)

	expected := testutils.Merge(t, testContent, map[string]interface{}{})

	/* Assert */
	result, err := testutils.GetGeneratorFile(out.RootDir, ds.RootDir, testFile.Name())
	// fmt.Println(result)
	// fmt.Println(expected)
	assert.Nil(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, expected, result)
}

func Test_Snowflake_Sql_SingleTest_SingleDataSource(t *testing.T) {
	ds, out := testutils.BootstrapDirs()
	defer testutils.CleanupDir(ds, out)

	dataContent := strings.TrimSpace(`
select 'Gunnar' as name
	`)
	dataSourceFile, err := testutils.CreateFile(ds.D1, "datasource.sql", dataContent, map[string]interface{}{})
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

	testutils.Run(logger, &formatter.Config{Filetype: formatter.ParserInputTypeSql}, ds.RootDir, out.RootDir)

	m1 := testutils.MergeOptions{
		LineNumber: 4,
		Regex:      nil,
		Content:    dataContent,
	}

	expected := testutils.Merge(t, testContent, format.Values{"0": dataSourceFile.Name()}, m1)

	result, err := testutils.GetGeneratorFile(out.RootDir, ds.RootDir, testFile.Name())
	// fmt.Println(result)
	// fmt.Println(expected)
	assert.Nil(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, expected, result)
}

func Test_Snowflake_Sql_SingleTest_SingleDataSource_OneLineEndcall(t *testing.T) {
	ds, out := testutils.BootstrapDirs()
	defer testutils.CleanupDir(ds, out)

	dataContent := strings.TrimSpace(`
select 'Gunnar' as name
	`)
	dataSourceFile, err := testutils.CreateFile(ds.D1, "datasource.sql", dataContent, map[string]interface{}{})
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

	testutils.Run(logger, &formatter.Config{Filetype: formatter.ParserInputTypeSql}, ds.RootDir, out.RootDir)

	m1 := testutils.MergeOptions{
		LineNumber: 4,
		Regex:      regexp.MustCompile(regexp.QuoteMeta("{'source_file': '${0}' }) %}")),
		Content:    dataContent,
	}

	expected := testutils.Merge(t, testContent, format.Values{"0": dataSourceFile.Name()}, m1)

	result, err := testutils.GetGeneratorFile(out.RootDir, ds.RootDir, testFile.Name())
	// fmt.Println(result)
	// fmt.Println(expected)
	assert.Nil(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, expected, result)
}

func Test_Snowflake_Sql_SingleTest_MultipleDataSources(t *testing.T) {
	ds, out := testutils.BootstrapDirs()
	defer testutils.CleanupDir(ds, out)

	firstContent := strings.TrimSpace(`
select 'Kåre' as name
	`)
	firstSourceFile, err := testutils.CreateFile(ds.D1, "first.sql", firstContent, map[string]interface{}{})
	if err != nil {
		t.Fatalf("Error creating data source file: %s", err)
	}

	secondContent := strings.TrimSpace(`
select 'Bjørn' as name
	`)
	secondSourceFile, err := testutils.CreateFile(ds.D2, "second.sql", secondContent, map[string]interface{}{})
	if err != nil {
		t.Fatalf("Error creating data source file: %s", err)
	}

	testContent := strings.TrimSpace(`
{{ config(tags=['unit-test']) }}

{% call dbt_unit_testing.test('<model-name>', '<test-name>') %}

	{% call dbt_unit_testing.mock_ref ('source_1', {'source_file': '${0}' }) %}
	{% endcall %}

	{% call dbt_unit_testing.mock_ref ('source_2', {'source_file': '${1}' }) %}
	{% endcall %}

	{% call dbt_unit_testing.expect() %}
select 'Gunnar' as name
	{% endcall %}

{% endcall %}
`)

	formats := format.Values{"0": firstSourceFile.Name(), "1": secondSourceFile.Name()}
	testFile, err := testutils.CreateFile(ds.D1, "test_snowflake.sql", testContent, formats)
	if err != nil {
		t.Fatalf("Error creating test file: %s", err)
	}

	testutils.Run(logger, &formatter.Config{Filetype: formatter.ParserInputTypeSql}, ds.RootDir, out.RootDir)

	m1 := testutils.MergeOptions{
		LineNumber: 4,
		Regex:      nil,
		Content:    firstContent,
	}
	m2 := testutils.MergeOptions{
		LineNumber: 7,
		Regex:      nil,
		Content:    secondContent,
	}

	expected := testutils.Merge(t, testContent, formats, m1, m2)

	result, err := testutils.GetGeneratorFile(out.RootDir, ds.RootDir, testFile.Name())
	fmt.Println(result)
	fmt.Println(expected)
	assert.Nil(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, expected, result)
}

func Test_Snowflake_Sql_SingleTest_MultipleDataSources_OneSharedDataSource(t *testing.T) {
	ds, out := testutils.BootstrapDirs()
	defer testutils.CleanupDir(ds, out)

	firstContent := strings.TrimSpace(`
select 'First' as name
	`)
	firstSourceFile, err := testutils.CreateFile(ds.D1, "first.sql", firstContent, map[string]interface{}{})
	if err != nil {
		t.Fatalf("Error creating data source file: %s", err)
	}

	secondContent := strings.TrimSpace(`
select 'Second' as name
	`)
	secondSourceFile, err := testutils.CreateFile(ds.D2, "second.sql", secondContent, map[string]interface{}{})
	if err != nil {
		t.Fatalf("Error creating data source file: %s", err)
	}

	thirdContent := strings.TrimSpace(`
select 'Third' as name
	`)
	thirdSourceFile, err := testutils.CreateFile(ds.RootDir, "third.sql", thirdContent, map[string]interface{}{})
	if err != nil {
		t.Fatalf("Error creating data source file: %s", err)
	}

	firstTestContent := strings.TrimSpace(`
{{ config(tags=['unit-test']) }}

{% call dbt_unit_testing.test('<model-name>', '<test-name>') %}

	{% call dbt_unit_testing.mock_ref ('first', {'source_file': '${0}' }) %}
	{% endcall %}

	{% call dbt_unit_testing.mock_ref ('second', {'source_file': '${1}' }) %}
	{% endcall %}

	{% call dbt_unit_testing.expect() %}
select 'First test' as name
	{% endcall %}

{% endcall %}
`)

	firstFormats := format.Values{"0": firstSourceFile.Name(), "1": secondSourceFile.Name()}
	firstTestFile, err := testutils.CreateFile(ds.D1, "test_first.sql", firstTestContent, firstFormats)
	if err != nil {
		t.Fatalf("Error creating test file: %s", err)
	}

	secondTestContent := strings.TrimSpace(`
{{ config(tags=['unit-test']) }}

{% call dbt_unit_testing.test('<model-name>', '<test-name>') %}

	{% call dbt_unit_testing.mock_ref ('second', {'source_file': '${0}' }) %}
	{% endcall %}

	{% call dbt_unit_testing.mock_ref ('third', {'source_file': '${1}' }) %}
	{% endcall %}

	{% call dbt_unit_testing.expect() %}
select 'Second test' as name
	{% endcall %}

{% endcall %}
`)
	secondFormats := format.Values{"0": secondSourceFile.Name(), "1": thirdSourceFile.Name()}
	secondTestFile, err := testutils.CreateFile(ds.D2, "test_second.sql", secondTestContent, secondFormats)
	if err != nil {
		t.Fatalf("Error creating test file: %s", err)
	}

	testutils.Run(logger, &formatter.Config{Filetype: formatter.ParserInputTypeSql}, ds.RootDir, out.RootDir)

	t1s1 := testutils.MergeOptions{LineNumber: 4, Regex: nil, Content: firstContent}
	t1s2 := testutils.MergeOptions{LineNumber: 7, Regex: nil, Content: secondContent}
	t1Expected := testutils.Merge(t, firstTestContent, firstFormats, t1s1, t1s2)
	t1Result, err := testutils.GetGeneratorFile(out.RootDir, ds.RootDir, firstTestFile.Name())
	assert.Nil(t, err)
	assert.NotNil(t, t1Result)
	assert.Equal(t, t1Expected, t1Result)

	t2s1 := testutils.MergeOptions{LineNumber: 4, Regex: nil, Content: secondContent}
	t2s2 := testutils.MergeOptions{LineNumber: 7, Regex: nil, Content: thirdContent}
	t2Expected := testutils.Merge(t, secondTestContent, secondFormats, t2s1, t2s2)
	t2Result, err := testutils.GetGeneratorFile(out.RootDir, ds.RootDir, secondTestFile.Name())
	assert.Nil(t, err)
	assert.NotNil(t, t2Result)
	assert.Equal(t, t2Expected, t2Result)
}
