# Snowflake Time CSV Parser

The `time` package provides an implementation of the `formatter.ICsvHeader` interface for handling numeric data types in CSV headers, specifically tailored for Snowflake SQL generation.

## Header Annotation

Annotations for time types must follow the pattern `<fieldname>[time(<format>,<precision>)]` where:

The signature for a time field is expected to have the format `<field_name>[time(<format>,<precision>)]`.

- `<format>`: (Optional) Specifies the format in which the time is provided. If not specified, the default format "15:04:05" is used.
- `<precision>`: (Optional) Indicates the fractional seconds precision for the time type, ranging from 0 to 9. If not specified, the default precision is 9.

Here are the predefined format strings you can use:

- "HH:mm:ss": "14:30:45"
- "hh:mm:ss tt": "02:30:45 PM"
- "HH:mm": "14:30"
- "hh:mm tt": "02:30 PM"
- "HH:mm:ss.SSS": "14:30:45.123"
- "hh:mm:ss.SSS tt": "02:30:45.123 PM"
- "HH:mm:ssZ": "14:30:45Z"
- "hh:mm:ss ttZ": "02:30:45 PMZ"
- "HH:mm:ss.SSSZ": "14:30:45.123Z"
- "hh:mm:ss.SSS ttZ": "02:30:45.123 PMZ"

**NOTE:** The time representations are case-sensitive and all fields without annotations are assumed to be of type varchar.

## Output

Given the following input CSV file:

```csv
timeField[time(HH:mm:ss tt)],anotherField
"14:30:45 PM","some text"
"02:30:45 PM","more text"
```

The package will produce the following Snowflake SQL output:

```sql
SELECT '14:30:45'::TIME(9) AS TIMEFIELD, 'some text'::VARCHAR(16777216) AS ANOTHERFIELD
UNION ALL
SELECT '02:30:45'::TIME(9) AS TIMEFIELD, 'more text'::VARCHAR(16777216) AS ANOTHERFIELD
```
