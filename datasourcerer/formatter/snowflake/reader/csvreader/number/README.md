# Snowflake Number CSV Parser

The `number` package provides an implementation of the `formatter.ICsvHeader` interface for handling numeric data types in CSV headers, specifically tailored for Snowflake SQL generation.

## Header Annotation

The signature for a number field is expected to have the format `<field_name>[number(<precision>,<scale>)]`. Both `<precision>` and `<scale>` are optional. If no precision is provided, it defaults to 38. If no scale is provided, it defaults to 2.

- `<precision>`: Total number of digits.
- `<scale>`: Number of digits to the right of the decimal point.

**NOTE:** The number representations are case-sensitive and all fields without annotations are assumed to be of type varchar.

## Output

Given the following input CSV file (note the quoted annotations):

```csv
Amount[number()],"Tax[number(5,2)]","Total[number(10,2)]"
100,7.5,107.50
200,15,215
```

The package will produce the following Snowflake SQL output:

```sql
SELECT 100::NUMBER(38,2) AS AMOUNT, 7.5::NUMBER(5,2) AS TAX, 107.50::NUMBER(10,2) AS TOTAL
UNION ALL
SELECT 200::NUMBER(38,2) AS AMOUNT, 15::NUMBER(5,2) AS TAX, 215::NUMBER(10,2) AS TOTAL
```
