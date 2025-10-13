# SFTP Connector Patterns - Exact Templates

**IMPORTANT: This document contains EXACT, production-ready templates for SFTP file-based ingestion. Copy character-for-character, line-by-line. Do NOT improvise or simplify.**

---

## Overview

SFTP connector patterns are used for **file-based data ingestion** from SFTP servers. Unlike API connectors, SFTP connectors read CSV/TSV files and load them as-is with NO incremental processing logic.

### When to Use SFTP Pattern

Use SFTP connector when:
- ✅ Data source provides CSV/TSV files via SFTP
- ✅ Files are complete snapshots or new data only
- ✅ No API available or file-based is preferred
- ✅ Simple append or replace mode is sufficient

Do NOT use SFTP connector when:
- ❌ You need incremental processing with start_time/end_time
- ❌ You need to query API for specific date ranges
- ❌ You need to deduplicate or merge data
- ❌ Files are in non-CSV format (JSON, XML, etc.)

---

## Key Characteristics

| Feature | SFTP Pattern | API Pattern |
|---------|-------------|-------------|
| **Connector Type** | `type: sftp_v2` | `type: {source}_api` |
| **Incremental** | No | Yes |
| **Time Tracking** | Upload time only | start_time/end_time |
| **Parallel** | No | Yes (configurable) |
| **Datasources File** | No | Yes |
| **File Path** | Secret or parameter | N/A |
| **Complexity** | Low | Medium-High |

---

## SFTP Workflow Pattern

### Complete Workflow Template

**File: `{source}_ingest.dig`**

```yaml
timezone: UTC

_export:
  !include : config/database.yml

+log_ingestion_start:
  td>: sql/log_ingestion_start.sql
  database: ${databases.src}
  source_name: '{source}'
  workflow_name: '{source}_ingest'

+load_{source}_{object1}:
  td_load>: config/{source}_{object1}_load.yml
  database: ${databases.src}
  table: {source}_{object1}
  file_path: ${secret:{source}_{object1}_file_path}
  fld_delimiter: ','
  mode: append

+load_{source}_{object2}:
  td_load>: config/{source}_{object2}_load.yml
  database: ${databases.src}
  table: {source}_{object2}
  file_path: ${secret:{source}_{object2}_file_path}
  fld_delimiter: ','
  mode: append

+log_ingestion_success:
  td>: sql/log_ingestion_success.sql
  database: ${databases.src}
  source_name: '{source}'
  workflow_name: '{source}_ingest'
  table_name: '{source}_{object1},{source}_{object2}'

_error:
  +log_ingestion_error:
    td>: sql/log_ingestion_error.sql
    database: ${databases.src}
    source_name: '{source}'
    workflow_name: '{source}_ingest'
    error_message: '${error.message}'
```

**Template Placeholders:**
- `{source}`: Source system name (e.g., pinterest, salesforce_reports)
- `{object1}`, `{object2}`: Data objects/files to load
- `table_name`: Comma-separated list of all tables

**Key Differences from API Pattern:**
- ✅ NO datasources.yml file
- ✅ NO parallel processing
- ✅ File path from secrets
- ✅ Simple sequential loading
- ✅ No start_time/end_time parameters

---

## SFTP Load Config Template

### Complete Load Config Template

**File: `config/{source}_{object}_load.yml`**

```yaml
in:
  type: sftp_v2
  host: '${secret:{source}_sftp_host}'
  port: 22
  auth_method: password
  user: '${secret:{source}_sftp_user}'
  password: '${secret:{source}_sftp_password}'
  user_directory_is_root: true
  timeout: 600
  path_prefix: ${file_path}
  parser:
    skip_header_lines: 1
    charset: 'UTF-8'
    newline: CRLF
    type: csv
    delimiter: '${fld_delimiter}'
    quote: "\""
    escape: "\""
    default_timezone: UTC
    use_string_literal: false
    trim_if_not_quoted: false
    stop_on_invalid_record: false
    quotes_in_quoted_fields: ACCEPT_ONLY_RFC4180_ESCAPED
    allow_extra_columns: false
    allow_optional_columns: false
    columns:
      - {name: "column1", type: "string"}
      - {name: "column2", type: "long"}
      - {name: "column3", type: "double"}
      - {name: "column4", type: "timestamp", format: "%Y-%m-%d %H:%M:%S"}

filters:
  - type: add_time
    to_column:
      name: time
      type: timestamp
    from_value:
      mode: upload_time

out:
  mode: ${mode}
```

**Template Placeholders:**
- `{source}`: Source system name
- `{object}`: Data object/file name
- `columns`: Define based on CSV structure (see Column Types section)
- `${file_path}`: From workflow parameter (secret)
- `${fld_delimiter}`: From workflow parameter
- `${mode}`: From workflow parameter (append/replace)

---

## Column Types and Formats

### Supported Column Types

| Type | Description | Example Value | YAML Example |
|------|-------------|---------------|--------------|
| `string` | Text data | "John Doe" | `{name: "customer_name", type: "string"}` |
| `long` | Integer | 12345 | `{name: "customer_id", type: "long"}` |
| `double` | Decimal | 123.45 | `{name: "amount", type: "double"}` |
| `boolean` | True/False | true | `{name: "is_active", type: "boolean"}` |
| `timestamp` | Date/time with format | "2024-01-01 00:00:00" | `{name: "created_at", type: "timestamp", format: "%Y-%m-%d %H:%M:%S"}` |
| `date` | Date only | "2024-01-01" | `{name: "order_date", type: "date", format: "%Y-%m-%d"}` |
| `json` | JSON string | '{"key":"value"}' | `{name: "metadata", type: "json"}` |

### Timestamp Format Codes

| Code | Meaning | Example |
|------|---------|---------|
| `%Y` | 4-digit year | 2024 |
| `%m` | 2-digit month | 01-12 |
| `%d` | 2-digit day | 01-31 |
| `%H` | 2-digit hour (24h) | 00-23 |
| `%M` | 2-digit minute | 00-59 |
| `%S` | 2-digit second | 00-59 |
| `%f` | Microseconds | 000000-999999 |

### Common Timestamp Formats

| Format String | Example | Use Case |
|---------------|---------|----------|
| `"%Y-%m-%d %H:%M:%S"` | "2024-01-01 12:30:45" | Standard datetime |
| `"%Y-%m-%dT%H:%M:%S"` | "2024-01-01T12:30:45" | ISO 8601 (no Z) |
| `"%Y-%m-%dT%H:%M:%S.%fZ"` | "2024-01-01T12:30:45.123456Z" | ISO 8601 with microseconds |
| `"%m/%d/%Y"` | "01/31/2024" | US date format |
| `"%d-%m-%Y"` | "31-01-2024" | European date format |

---

## Parser Settings Reference

### Core Parser Settings

| Setting | Default | Purpose | Common Values |
|---------|---------|---------|---------------|
| `skip_header_lines` | 1 | Skip CSV headers | 0, 1, 2 |
| `charset` | 'UTF-8' | File encoding | 'UTF-8', 'ISO-8859-1', 'Shift_JIS' |
| `newline` | CRLF | Line ending style | CRLF (Windows), LF (Unix) |
| `type` | csv | File type | csv, tsv |
| `delimiter` | ',' | Field delimiter | ',', '\t', '|', ';' |
| `quote` | "\"" | Quote character | "\"", "'" |
| `escape` | "\"" | Escape character | "\"", "\\" |

### Advanced Parser Settings

| Setting | Default | Purpose |
|---------|---------|---------|
| `default_timezone` | UTC | Timezone for timestamps |
| `use_string_literal` | false | Treat all as strings first |
| `trim_if_not_quoted` | false | Trim whitespace from unquoted fields |
| `stop_on_invalid_record` | false | Stop on parse errors |
| `quotes_in_quoted_fields` | ACCEPT_ONLY_RFC4180_ESCAPED | How to handle quotes |
| `allow_extra_columns` | false | Allow more columns than defined |
| `allow_optional_columns` | false | Allow missing columns |

### Quote Handling Options

| Value | Behavior |
|-------|----------|
| `ACCEPT_ONLY_RFC4180_ESCAPED` | Only accept RFC 4180 escaped quotes ("") |
| `ACCEPT_STRAY_QUOTES_ASSUMING_NO_DELIMITERS_IN_FIELDS` | Accept unescaped quotes |

---

## Authentication Methods

### Password Authentication (Most Common)

```yaml
in:
  type: sftp_v2
  host: '${secret:source_sftp_host}'
  port: 22
  auth_method: password
  user: '${secret:source_sftp_user}'
  password: '${secret:source_sftp_password}'
```

### SSH Key Authentication

```yaml
in:
  type: sftp_v2
  host: '${secret:source_sftp_host}'
  port: 22
  auth_method: key_pair
  user: '${secret:source_sftp_user}'
  private_key: '${secret:source_sftp_private_key}'
  passphrase: '${secret:source_sftp_passphrase}'  # Optional
```

---

## Credentials Configuration

### Required Secrets (Password Auth)

Add to `credentials_ingestion.json`:

```json
{
  "{source}_sftp_host": "sftp.example.com",
  "{source}_sftp_user": "username",
  "{source}_sftp_password": "password",
  "{source}_{object1}_file_path": "/path/to/file1.csv",
  "{source}_{object2}_file_path": "/path/to/file2.csv"
}
```

### Required Secrets (SSH Key Auth)

```json
{
  "{source}_sftp_host": "sftp.example.com",
  "{source}_sftp_user": "username",
  "{source}_sftp_private_key": "-----BEGIN RSA PRIVATE KEY-----\n...\n-----END RSA PRIVATE KEY-----",
  "{source}_sftp_passphrase": "optional_passphrase",
  "{source}_{object1}_file_path": "/path/to/file1.csv"
}
```

### Upload Secrets

```bash
cd ingestion/
td wf secrets --project ingestion --set @credentials_ingestion.json
```

---

## File Path Patterns

### Static File Path

When file name is always the same:

```yaml
# In workflow
file_path: ${secret:source_object_file_path}

# In credentials
{
  "source_object_file_path": "/data/customers.csv"
}
```

### Dynamic File Path with Date

When file name includes date:

```yaml
# In workflow
file_path: "/data/customers_${session_date}.csv"
session_date: "2024-01-31"

# No credential needed - constructed in workflow
```

### Wildcard File Path

When processing multiple files:

```yaml
# In workflow
file_path: "/data/customers_*.csv"

# Loads all matching files
```

---

## Common SFTP Patterns

### Pattern 1: Simple Append

Load new data and append to existing table:

```yaml
+load_data:
  td_load>: config/source_object_load.yml
  database: ${databases.src}
  table: source_object
  file_path: ${secret:source_object_file_path}
  fld_delimiter: ','
  mode: append
```

### Pattern 2: Full Replace

Replace entire table with new data:

```yaml
+load_data:
  td_load>: config/source_object_load.yml
  database: ${databases.src}
  table: source_object
  file_path: ${secret:source_object_file_path}
  fld_delimiter: ','
  mode: replace
```

### Pattern 3: Multiple Files Sequential

Load multiple files one after another:

```yaml
+load_file1:
  td_load>: config/source_object1_load.yml
  database: ${databases.src}
  table: source_object1
  file_path: ${secret:source_object1_file_path}
  fld_delimiter: ','
  mode: append

+load_file2:
  td_load>: config/source_object2_load.yml
  database: ${databases.src}
  table: source_object2
  file_path: ${secret:source_object2_file_path}
  fld_delimiter: ','
  mode: append
```

---

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| "Connection refused" | Wrong host/port | Verify SFTP host and port (usually 22) |
| "Authentication failed" | Wrong credentials | Verify username/password or SSH key |
| "File not found" | Wrong file path | Check file path, case-sensitive on Linux |
| "Column count mismatch" | CSV structure changed | Update `columns` definition in config |
| "Parse error at line X" | Invalid CSV format | Check for unescaped quotes, extra delimiters |
| "Timeout" | Large file | Increase `timeout` setting (default 600) |
| "Permission denied" | Wrong file permissions | Check SFTP user has read access |

### Debugging Steps

1. **Test SFTP connection manually:**
   ```bash
   sftp -P 22 user@sftp.example.com
   ls /path/to/files
   get /path/to/file.csv
   exit
   ```

2. **Verify file structure:**
   - Download sample file
   - Open in text editor (not Excel - may corrupt)
   - Check delimiter (comma, tab, pipe, etc.)
   - Count columns vs. config
   - Check for special characters

3. **Test with small file first:**
   - Create test file with 10-100 rows
   - Upload to SFTP
   - Run workflow
   - Verify data loaded correctly

4. **Check secrets:**
   ```bash
   td wf secrets --project ingestion
   ```

5. **Check ingestion log:**
   ```sql
   SELECT * FROM mck_src.ingestion_log
   WHERE source_name = '{source}'
   ORDER BY time DESC
   LIMIT 10
   ```

---

## Best Practices

### Column Definition

✅ **DO:**
- Define all columns explicitly
- Use correct types for each column
- Include timestamp format for date/time columns
- Test with sample data first

❌ **DON'T:**
- Use `allow_extra_columns: true` unless necessary
- Guess at column types
- Skip timestamp format specification
- Assume column order stays same

### File Handling

✅ **DO:**
- Use meaningful file paths
- Check file exists before loading
- Handle timezone correctly in timestamps
- Use `upload_time` for time column

❌ **DON'T:**
- Hardcode file paths in configs
- Process files without headers
- Mix date formats in same column
- Use relative file paths

### Error Handling

✅ **DO:**
- Always include `_error` block
- Log all ingestion attempts
- Monitor ingestion_log table
- Set reasonable timeout values

❌ **DON'T:**
- Set `stop_on_invalid_record: true` without understanding
- Ignore parse errors
- Skip error logging
- Use default timeout for large files

---

## Production Checklist

- [ ] SFTP connection tested manually
- [ ] Sample file downloaded and structure verified
- [ ] All columns defined with correct types
- [ ] Timestamp formats specified correctly
- [ ] Delimiter setting matches file
- [ ] Secrets uploaded via `td wf secrets`
- [ ] Workflow syntax checked: `td wf check {source}_ingest.dig`
- [ ] Test run with small file completed
- [ ] Data verified in target table
- [ ] Logging verified in ingestion_log table
- [ ] Full file loaded successfully
- [ ] Monitoring and alerting configured

---

## Quick Reference

### Minimum Load Config

```yaml
in:
  type: sftp_v2
  host: '${secret:source_sftp_host}'
  port: 22
  auth_method: password
  user: '${secret:source_sftp_user}'
  password: '${secret:source_sftp_password}'
  path_prefix: ${file_path}
  parser:
    type: csv
    delimiter: ','
    columns:
      - {name: "id", type: "long"}
      - {name: "name", type: "string"}

filters:
  - type: add_time
    to_column: {name: time, type: timestamp}
    from_value: {mode: upload_time}

out:
  mode: append
```

### Common Delimiters

| Delimiter | YAML | Description |
|-----------|------|-------------|
| Comma | `','` | Standard CSV |
| Tab | `'\t'` | TSV files |
| Pipe | `'|'` | Pipe-delimited |
| Semicolon | `';'` | European CSV |

---

## Real-World Examples

See `docs/sources/pinterest.md` for a complete, production-tested SFTP implementation.

---

## Remember

✅ **DO:**
- Copy templates exactly as shown
- Define all columns accurately
- Use secrets for all credentials
- Test with small files first
- Monitor ingestion_log table
- Use exact parser settings from template

❌ **DON'T:**
- Modify parser settings without understanding
- Hardcode credentials
- Skip column type specification
- Assume file structure
- Use SFTP for incremental processing needs

---

**When in doubt, copy the template exactly. These templates are production-tested.**
