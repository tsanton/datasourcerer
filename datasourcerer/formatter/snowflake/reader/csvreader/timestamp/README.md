# Snowflake Timestamp CSV Parser

The `timestamp` package provides an implementation of the `formatter.ICsvHeader` interface for handling timestamp data types in CSV headers, specifically tailored for Snowflake SQL generation.

## Header Annotation

Annotations for timestamp types must follow the pattern `<fieldname>[timestamp(<format>,<precision>,<timezone>)]` where:

The signature for a timestamp field is expected to have the format `<field_name>[timestamp(<format>,<precision>,<timezone>)]`.

- `<format>`: (Optional) Specifies the format in which the timestamp is provided. If not specified, the default format "yyyy-MM-dd HH:mm:ss" is used.
- `<precision>`: (Optional) Indicates the fractional seconds precision for the timestamp type, ranging from 0 to 9. If not specified, the default precision is 9.
- `<timezone>`: (Optional) Specifies the timezone of the timestamp. If not specified, the default timezone "UTC" is used.

Here are the predefined format strings you can use:

- "yyyy-MM-dd HH:mm:ss": "2023-10-24 14:30:45"
- "yyyy-MM-dd hh:mm:ss tt": "2023-10-24 02:30:45 PM"
- "yyyy-MM-dd HH:mm": "2023-10-24 14:30"
- "yyyy-MM-dd hh:mm tt": "2023-10-24 02:30 PM"
- "yyyy-MM-dd HH:mm:ss.SSS": "2023-10-24 14:30:45.123"
- "yyyy-MM-dd hh:mm:ss.SSS tt": "2023-10-24 02:30:45.123 PM"
- "yyyy-MM-dd HH:mm:ssZ": "2023-10-24 14:30:45Z"
- "yyyy-MM-dd hh:mm:ss ttZ": "2023-10-24 02:30:45 PMZ"
- "yyyy-MM-dd HH:mm:ss.SSSZ": "2023-10-24 14:30:45.123Z"
- "yyyy-MM-dd hh:mm:ss.SSS ttZ": "2023-10-24 02:30:45.123 PMZ"

**NOTE:** The timestamp representations are case-sensitive and all fields without annotations are assumed to be of type varchar.

## Output

Given the following input CSV file:

```csv
timestampField[timestamp(yyyy-MM-dd HH:mm:ss tt)],anotherField
"2023-10-24 02:30:45 PM","some text"
"2023-10-24 02:30:45 PM","more text"
```

The package will produce the following Snowflake SQL output:

```sql
SELECT '2023-10-24 14:30:45'::TIMESTAMP_NTZ(9) AS TIMESTAMPFIELD, 'some text'::VARCHAR(16777216) AS ANOTHERFIELD
UNION ALL
SELECT '2023-10-24 14:30:45'::TIMESTAMP_NTZ(9) AS TIMESTAMPFIELD, 'more text'::VARCHAR(16777216) AS ANOTHERFIELD
```
