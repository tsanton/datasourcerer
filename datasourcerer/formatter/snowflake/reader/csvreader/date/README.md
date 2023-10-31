# Snowflake Date CSV Package

The `date` package provides an implementation of the `formatter.ICsvHeader` interface for handling date data types in CSV headers, specifically tailored for Snowflake SQL generation.

## Header Annotation

The signature for a date field is expected to have the format `<field_name>[date(<format>)]`. The `<format>` is optional and can be any valid Go time format string or a predefined format string. If no format is provided, it defaults to "2006-01-02".

Here are the predefined format strings you can use:

- "yyyy-MM-dd": "2006-01-02"
- "dd-MM-yyyy": "02-01-2006"
- "MM/dd/yyyy": "01/02/2006"
- "yyyy/MM/dd": "2006/01/02"
- "dd/MM/yyyy": "02/01/2006"
- "MMM dd, yyyy": "Jan 02, 2006"
- "MMMM dd, yyyy": "January 02, 2006"
- "dd MMM yyyy": "02 Jan 2006"
- "yyyy-MM-ddTHH:mm:ssZ": "2006-01-02T15:04:05Z07:00"

**NOTE:** The date representations are case-sensitive and all fields without annotations are assumed to be of type varchar.

### Example

Given the following input CSV file:

```csv
Date[date(yyyy-MM-dd)],Event
2023-10-31,Halloween
2023-12-25,Christmas
```

The package will produce the following Snowflake SQL output:

```sql
SELECT '2023-10-31'::DATE AS DATE, 'Halloween'::VARCHAR(16777216) AS EVENT
UNION ALL
SELECT '2023-12-25'::DATE AS DATE, 'Christmas'::VARCHAR(16777216) AS EVENT
```
