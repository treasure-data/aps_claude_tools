# Timestamp Format Patterns

## Overview
Different connectors require different timestamp formats. Using the WRONG format will cause ingestion failures. This document provides the EXACT timestamp conversion functions for each connector.

## Critical Rule
**NEVER guess timestamp formats. ALWAYS use the exact functions documented here for each source.**

---

## Timestamp Format by Source

| Source | Start Time Format | End Time Format | Decimals | Timezone Marker |
|--------|------------------|-----------------|----------|-----------------|
| **Klaviyo** | `.000000` | `.000000` | 6 | None |
| **OneTrust** | `.000Z` | `.000Z` | 3 | Z |
| **Shopify v2** | ISO 8601 | ISO 8601 | 3 | Z |
| **Google BigQuery** | SQL Server style | SQL Server style | 3 | Space separator |

---

## Exact SQL Functions by Source

### 1. Klaviyo Timestamp Functions

**Format**: `.000000` (6 decimals, NO Z suffix)

#### Start Time (Get Max from Table)
```sql
SELECT
  COALESCE(
    (SELECT replace(TD_TIME_STRING(TD_TIME_PARSE(max(${datasource.incremental_field})), 's!'), ' ', 'T') || '.000000' as start_time
      FROM ${databases.src}.${datasource.table_name}
      WHERE ${datasource.incremental_field} IS NOT NULL),
    '${datasource.default_start_time}'
  ) as start_time,
  SUBSTR(CAST(to_iso8601(current_timestamp) AS VARCHAR),1,19)||'.000000' as end_time
```

#### End Time (Current Timestamp)
```sql
SUBSTR(CAST(to_iso8601(current_timestamp) AS VARCHAR),1,19)||'.000000' as end_time
```

**Example Output**:
- Start: `2023-09-01T00:00:00.000000`
- End: `2025-10-10T13:45:30.000000`

---

### 2. OneTrust Timestamp Functions

**Format**: `.000Z` (3 decimals, WITH Z suffix)

#### Start Time (Get Max from Table)
```sql
SELECT
  COALESCE(
    (SELECT replace(to_iso8601(TD_TIME_PARSE(max(${datasource.incremental_field}))), 'Z', '.000Z') as start_time
     FROM ${databases.src}.${datasource.table_name}
     WHERE ${datasource.incremental_field} IS NOT NULL),
    '${datasource.default_start_time}'
  ) as start_time,
  replace(to_iso8601(current_timestamp), 'Z', '.000Z') as end_time
```

#### End Time (Current Timestamp)
```sql
replace(to_iso8601(current_timestamp), 'Z', '.000Z') as end_time
```

**Example Output**:
- Start: `2023-09-01T00:00:00.000Z`
- End: `2025-10-10T13:45:30.000Z`

---

### 3. Shopify v2 Timestamp Functions

**Format**: ISO 8601 standard (3 decimals, WITH Z suffix)

#### Start Time (Get Max from Table)
```sql
SELECT
  COALESCE(
    (SELECT replace(TD_TIME_STRING(TD_TIME_PARSE(max(${datasource.incremental_field})), 's!'), ' ', 'T') || '.000Z' as start_time
      FROM ${databases.src}.${datasource.table_name}
      WHERE ${datasource.incremental_field} IS NOT NULL),
    '${datasource.default_start_time}'
  ) as start_time,
  to_iso8601(current_timestamp) as end_time
```

#### End Time (Current Timestamp)
```sql
to_iso8601(current_timestamp) as end_time
```

**Example Output**:
- Start: `2023-09-01T00:00:00.000Z`
- End: `2025-10-10T13:45:30.123Z`

---

### 4. Google BigQuery Timestamp Functions

**Format**: SQL Server style (`yyyy-MM-dd HH:mm:ss.SSS`)

#### Start Time (Get Max from Table)
```sql
SELECT
  COALESCE(MAX(${datasource.inc_field}), '${datasource.default_start_time}') as start_time,
  FORMAT_DATETIME(CURRENT_TIMESTAMP, 'yyyy-MM-dd'' ''HH:mm:ss.SSS') as end_time
FROM ${datasource.table}
```

#### End Time (Current Timestamp)
```sql
FORMAT_DATETIME(CURRENT_TIMESTAMP, 'yyyy-MM-dd'' ''HH:mm:ss.SSS') as end_time
```

**Example Output**:
- Start: `2025-08-31 23:59:59.000`
- End: `2025-10-10 13:45:30.123`

**Note**: Uses space separator, NOT 'T'

---

## Complete SQL Query Patterns by Source

### Klaviyo - Get Last Incremental Time
```sql
+get_last_incremental_time:
  td>:
  query: |
    SELECT
      COALESCE(
        (SELECT replace(TD_TIME_STRING(TD_TIME_PARSE(max(${datasource.incremental_field})), 's!'), ' ', 'T') || '.000000' as start_time
          FROM ${databases.src}.${datasource.table_name}
          WHERE ${datasource.incremental_field} IS NOT NULL),
        '${datasource.default_start_time}'
      ) as start_time,
      SUBSTR(CAST(to_iso8601(current_timestamp) AS VARCHAR),1,19)||'.000000' as end_time
  database: ${databases.src}
  store_last_results: true
```

### OneTrust - Get Last Incremental Time
```sql
+get_last_incremental_time:
  td>:
  query: |
    SELECT
      COALESCE(
        (SELECT replace(to_iso8601(TD_TIME_PARSE(max(${datasource.incremental_field}))), 'Z', '.000Z') as start_time
         FROM ${databases.src}.${datasource.table_name}
         WHERE ${datasource.incremental_field} IS NOT NULL),
        '${datasource.default_start_time}'
      ) as start_time,
      replace(to_iso8601(current_timestamp), 'Z', '.000Z') as end_time
  database: ${databases.src}
  store_last_results: true
```

### Shopify v2 - Get Last Incremental Time
```sql
+get_last_incremental_time:
  td>:
  query: |
    SELECT
      COALESCE(
        (SELECT replace(TD_TIME_STRING(TD_TIME_PARSE(max(${datasource.incremental_field})), 's!'), ' ', 'T') || '.000Z' as start_time
          FROM ${databases.src}.${datasource.table_name}
          WHERE ${datasource.incremental_field} IS NOT NULL),
        '${datasource.default_start_time}'
      ) as start_time,
      to_iso8601(current_timestamp) as end_time
  database: ${databases.src}
  store_last_results: true
```

### Google BigQuery - Get Max Incremental Field
```sql
+get_max_inc_field:
  td>:
  query: |
    SELECT
      COALESCE(MAX(${datasource.inc_field}), '${datasource.default_start_time}') as start_time,
      FORMAT_DATETIME(CURRENT_TIMESTAMP, 'yyyy-MM-dd'' ''HH:mm:ss.SSS') as end_time
    FROM ${datasource.table}
  database: ${databases.src}
  store_last_results: true
```

---

## Default Start Time Formats by Source

### Klaviyo
```yaml
default_start_time: "2023-09-01T00:00:00.000000"
```

### OneTrust
```yaml
default_start_time: "2025-01-30T00:49:04Z"
```

### Shopify v2
```yaml
default_start_time: "2023-01-01T00:00:00.000Z"
```

### Google BigQuery
```yaml
default_start_time: "2025-08-31 23:59:59.000"
```

---

## Non-Incremental Sources (Empty Start Time)

For sources without incremental fields (e.g., Klaviyo metrics):

```sql
+set_default_time:
  td>:
  query: |
    SELECT
      '' as start_time,
      [END_TIME_FUNCTION] as end_time
  database: ${databases.src}
  store_last_results: true
```

Replace `[END_TIME_FUNCTION]` with the appropriate function for your connector.

---

## Common Timestamp Conversion Functions

### TD_TIME_PARSE
Parses a timestamp string into Treasure Data's internal time format.

```sql
TD_TIME_PARSE(max(${datasource.incremental_field}))
```

### TD_TIME_STRING
Converts TD time to a string with specified format.

```sql
TD_TIME_STRING(TD_TIME_PARSE(max(${datasource.incremental_field})), 's!')
```

### to_iso8601
Converts timestamp to ISO 8601 format.

```sql
to_iso8601(current_timestamp)
```

### replace
Replaces substrings in a string.

```sql
replace(to_iso8601(current_timestamp), 'Z', '.000Z')
replace(TD_TIME_STRING(...), ' ', 'T')
```

### SUBSTR / CAST
Extracts substring or casts to VARCHAR.

```sql
SUBSTR(CAST(to_iso8601(current_timestamp) AS VARCHAR), 1, 19)
```

---

## Troubleshooting Timestamp Errors

### Error: "Invalid timestamp format"
**Cause**: Using wrong decimal count or timezone marker

**Solution**: Verify you're using the EXACT function for your source

### Error: "No data ingested" (but no error logged)
**Cause**: start_time > end_time or malformed timestamp

**Solution**:
1. Check default_start_time format matches connector
2. Verify timestamp functions match source
3. Test with fixed timestamps first

### Error: "Timestamp parse error"
**Cause**: Incremental field format doesn't match expected format

**Solution**:
1. Check actual data format in source table
2. Adjust TD_TIME_PARSE call if needed
3. Verify incremental_field name is correct

---

## Quick Reference Decision Tree

```
What connector am I using?
├─ Klaviyo
│   └─ Use: SUBSTR(CAST(to_iso8601(...),1,19)||'.000000'
├─ OneTrust
│   └─ Use: replace(to_iso8601(...), 'Z', '.000Z')
├─ Shopify v2
│   └─ Use: to_iso8601(...)
└─ Google BigQuery
    └─ Use: FORMAT_DATETIME(..., 'yyyy-MM-dd'' ''HH:mm:ss.SSS')

What am I converting?
├─ MAX(field) from table
│   └─ Use TD_TIME_PARSE first, then format
└─ Current timestamp
    └─ Use current_timestamp or CURRENT_TIMESTAMP
```

---

## Critical Reminders

1. **NEVER mix timestamp formats** between start and end time
2. **ALWAYS test with default_start_time** before deploying
3. **VERIFY decimal count** matches connector requirements exactly
4. **CHECK timezone marker** (Z vs. no Z) for your connector
5. **USE exact functions** - do not try to simplify or optimize
6. **STORE results** with `store_last_results: true` after time queries
