package unit_test

import (
	"regexp"
	"strings"
	"testing"

	"github.com/sirkon/go-format"
	"github.com/stretchr/testify/assert"
	"github.com/tsanton/dbt-unit-test-fusionizer/formatter"
	"github.com/tsanton/dbt-unit-test-fusionizer/testutils"
)

func Test_Snowflake_Sql_NoDataSources(t *testing.T) {
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

func Test_Snowflake_Sql_SingleDataSource(t *testing.T) {
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

func Test_Snowflake_Sql_SingleDataSource_OnelineEndcall(t *testing.T) {
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
