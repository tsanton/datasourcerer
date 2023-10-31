# Snowflake Varchar CSV parser

The varchar package provides an implementation of the formatter.ICsvHeader interface for handling varchar data types in CSV headers, specifically tailored for Snowflake SQL generation.

## Header annotation

The signature is expected to have the format `<field_name>[varchar(<size>)]`.

**NOTE:** `All fields without annotations are assumed to be of type varchar`

## Output

Given the following input CSV file:

```csv
Name,Address[varchar(10)],City[varchar()]
John,123 Main St,New York
Jane,456 Main St,New York
```

The package will produce the following Snowflake SQL output:

```sql
SELECT 'John'::VARCHAR(16777216) AS NAME, '123 Main St'::VARCHAR(10) AS ADDRESS, 'New York'::VARCHAR(16777216) AS CITY
UNION ALL
SELECT  'Jane'::VARCHAR(16777216) AS NAME, '456 Main St'::VARCHAR(10) AS ADDRESS, 'New York'::VARCHAR(16777216) AS CITY
UNION ALL
SELECT  'Kane'::VARCHAR(16777216) AS NAME, '901 Main St'::VARCHAR(10) AS ADDRESS, 'New York'::VARCHAR(16777216) AS CITY
```
