# Pinterest Data Ingestion - Exact Templates

**IMPORTANT: This document contains EXACT, production-ready templates. Copy character-for-character, line-by-line. Do NOT improvise or simplify.**

---

## Overview

Pinterest data ingestion uses **SFTP connector** to load CSV files from Pinterest's SFTP server. This is a **file-based ingestion** pattern with NO incremental processing - files are loaded as-is when workflow runs.

### Key Characteristics

| Feature | Value |
|---------|-------|
| **Connector Type** | SFTP v2 (file-based) |
| **Incremental** | No - append mode only |
| **Parallel** | No - sequential loading |
| **Timestamp Format** | Upload time (auto-generated) |
| **File Format** | CSV with headers |
| **Auth Method** | Password-based SFTP |
| **Tables** | campaigns, ad_groups |

---

## Critical Implementation Details

### ⚠️ CRITICAL: Key Differences from API Sources

1. **NO incremental processing** - No start_time/end_time logic
2. **NO datasources.yml file** - File paths configured as secrets
3. **File path is a secret** - Must be uploaded via td wf secrets
4. **Sequential loading** - No parallel processing
5. **Simple logging** - Only start, success, error (no time tracking)

---

## File Structure

```
ingestion/
├── pinterest_ingest.dig                   # Main workflow
├── config/
│   ├── database.yml                       # Shared database config
│   ├── pinterest_campaigns_load.yml       # Campaigns load config
│   └── pinterest_ad_groups_load.yml       # Ad groups load config
└── sql/
    ├── log_ingestion_start.sql            # Start logging
    ├── log_ingestion_success.sql          # Success logging
    └── log_ingestion_error.sql            # Error logging
```

---

## Complete File Templates

### 1. Workflow File: `pinterest_ingest.dig`

**EXACT TEMPLATE - Copy character-for-character:**

```yaml
timezone: UTC

_export:
  !include : config/database.yml

+log_ingestion_start:
  td>: sql/log_ingestion_start.sql
  database: ${databases.src}
  source_name: 'pinterest'
  workflow_name: 'pinterest_ingest'

+load_pinterest_campaigns:
  td_load>: config/pinterest_campaigns_load.yml
  database: ${databases.src}
  table: pinterest_campaigns
  file_path: ${secret:pinterest_campaigns_file_path}
  fld_delimiter: ','
  mode: append

+load_pinterest_ad_groups:
  td_load>: config/pinterest_ad_groups_load.yml
  database: ${databases.src}
  table: pinterest_ad_groups
  file_path: ${secret:pinterest_ad_groups_file_path}
  fld_delimiter: ','
  mode: append

+log_ingestion_success:
  td>: sql/log_ingestion_success.sql
  database: ${databases.src}
  source_name: 'pinterest'
  workflow_name: 'pinterest_ingest'
  table_name: 'pinterest_campaigns,pinterest_ad_groups'

_error:
  +log_ingestion_error:
    td>: sql/log_ingestion_error.sql
    database: ${databases.src}
    source_name: 'pinterest'
    workflow_name: 'pinterest_ingest'
    error_message: '${error.message}'
```

**Template Placeholders:**
- Source name: Always `'pinterest'`
- Workflow name: Always `'pinterest_ingest'`
- Table names: Comma-separated list in success log

---

### 2. Load Config: `pinterest_campaigns_load.yml`

**EXACT TEMPLATE - Copy character-for-character:**

```yaml
in:
  type: sftp_v2
  host: '${secret:pinterest_sftp_host}'
  port: 22
  auth_method: password
  user: '${secret:pinterest_sftp_user}'
  password: '${secret:pinterest_sftp_password}'
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
      - {name: "campaign_id", type: "string"}
      - {name: "campaign_name", type: "string"}
      - {name: "status", type: "string"}
      - {name: "budget", type: "double"}
      - {name: "start_date", type: "string"}
      - {name: "end_date", type: "string"}
      - {name: "created_time", type: "string"}
      - {name: "updated_time", type: "string"}

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
- `${file_path}`: From workflow parameter (secret)
- `${fld_delimiter}`: From workflow parameter
- `${mode}`: From workflow parameter
- `columns`: Define based on CSV structure

---

### 3. Load Config: `pinterest_ad_groups_load.yml`

**EXACT TEMPLATE - Copy character-for-character:**

```yaml
in:
  type: sftp_v2
  host: '${secret:pinterest_sftp_host}'
  port: 22
  auth_method: password
  user: '${secret:pinterest_sftp_user}'
  password: '${secret:pinterest_sftp_password}'
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
      - {name: "ad_group_id", type: "string"}
      - {name: "ad_group_name", type: "string"}
      - {name: "campaign_id", type: "string"}
      - {name: "status", type: "string"}
      - {name: "bid_strategy", type: "string"}
      - {name: "budget", type: "double"}
      - {name: "created_time", type: "string"}
      - {name: "updated_time", type: "string"}

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
- Same as campaigns config
- `columns`: Define based on CSV structure

---

## Credentials Configuration

### Required Secrets

Add to `credentials_ingestion.json`:

```json
{
  "pinterest_sftp_host": "sftp.pinterest.com",
  "pinterest_sftp_user": "your_username",
  "pinterest_sftp_password": "your_password",
  "pinterest_campaigns_file_path": "/path/to/campaigns.csv",
  "pinterest_ad_groups_file_path": "/path/to/ad_groups.csv"
}
```

Upload via:
```bash
td wf secrets --project ingestion --set @credentials_ingestion.json
```

---

## Adding New Pinterest Data Files

### Step 1: Create Load Config

File: `ingestion/config/pinterest_{object}_load.yml`

```yaml
in:
  type: sftp_v2
  host: '${secret:pinterest_sftp_host}'
  port: 22
  auth_method: password
  user: '${secret:pinterest_sftp_user}'
  password: '${secret:pinterest_sftp_password}'
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
      # DEFINE YOUR COLUMNS HERE
      - {name: "column1", type: "string"}
      - {name: "column2", type: "long"}

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

### Step 2: Update Workflow

Add new load task to `pinterest_ingest.dig` (before success log):

```yaml
+load_pinterest_{object}:
  td_load>: config/pinterest_{object}_load.yml
  database: ${databases.src}
  table: pinterest_{object}
  file_path: ${secret:pinterest_{object}_file_path}
  fld_delimiter: ','
  mode: append
```

### Step 3: Update Success Log

Update the `table_name` in success log to include new table:

```yaml
+log_ingestion_success:
  td>: sql/log_ingestion_success.sql
  database: ${databases.src}
  source_name: 'pinterest'
  workflow_name: 'pinterest_ingest'
  table_name: 'pinterest_campaigns,pinterest_ad_groups,pinterest_{object}'
```

### Step 4: Add Credentials

Add to `credentials_ingestion.json`:

```json
{
  "pinterest_{object}_file_path": "/path/to/{object}.csv"
}
```

---

## Common SFTP Parser Options

### Column Types

| Type | Description | Example |
|------|-------------|---------|
| `string` | Text data | "John Doe" |
| `long` | Integer | 12345 |
| `double` | Decimal | 123.45 |
| `boolean` | True/False | true |
| `timestamp` | Date/time | "2024-01-01 00:00:00" |

### Parser Settings

| Setting | Default | Purpose |
|---------|---------|---------|
| `skip_header_lines` | 1 | Skip CSV headers |
| `charset` | 'UTF-8' | File encoding |
| `newline` | CRLF | Windows line endings |
| `delimiter` | ',' | CSV delimiter |
| `stop_on_invalid_record` | false | Continue on errors |
| `allow_extra_columns` | false | Strict column matching |

---

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| "Connection refused" | Wrong SFTP host/port | Verify `pinterest_sftp_host` and port |
| "Authentication failed" | Wrong credentials | Verify `pinterest_sftp_user` and `pinterest_sftp_password` |
| "File not found" | Wrong file path | Verify `pinterest_{object}_file_path` |
| "Column mismatch" | CSV structure changed | Update `columns` in load config |
| "Parse error" | Wrong delimiter | Verify `fld_delimiter` matches CSV |

### Debugging Steps

1. **Verify secrets uploaded:**
   ```bash
   td wf secrets --project ingestion
   ```

2. **Test SFTP connection manually:**
   ```bash
   sftp -P 22 user@sftp.pinterest.com
   ```

3. **Check file structure:**
   - Download sample file
   - Verify delimiter (comma, tab, etc.)
   - Count columns vs. config

4. **Check ingestion log:**
   ```sql
   SELECT * FROM mck_src.ingestion_log
   WHERE source_name = 'pinterest'
   ORDER BY time DESC
   LIMIT 10
   ```

---

## Testing Checklist

- [ ] Secrets uploaded via `td wf secrets`
- [ ] SFTP connection tested
- [ ] File paths verified
- [ ] Column definitions match CSV structure
- [ ] Delimiter setting correct
- [ ] Workflow syntax checked: `td wf check pinterest_ingest.dig`
- [ ] Test run completed: `td wf run pinterest_ingest.dig`
- [ ] Data verified in target tables
- [ ] Logging verified in ingestion_log table

---

## Quick Reference

### Workflow Pattern

```
pinterest_ingest.dig
├── log_ingestion_start
├── load_pinterest_campaigns (sequential)
├── load_pinterest_ad_groups (sequential)
├── log_ingestion_success
└── _error → log_ingestion_error
```

### Key Differences from API Sources

| Feature | API Sources | Pinterest (SFTP) |
|---------|------------|------------------|
| Connector | API-specific | sftp_v2 |
| Incremental | Yes | No |
| Time tracking | start_time/end_time | upload_time only |
| Datasources file | Yes | No |
| Parallel | Yes (limit: 3) | No |
| File paths | N/A | Secrets |

---

## Production Deployment

### Deployment Checklist

1. Create all config files
2. Upload secrets
3. Test workflow syntax
4. Run test with small file
5. Verify data loaded
6. Schedule workflow (if needed)
7. Monitor ingestion_log

### Scheduling

To schedule daily/weekly runs:

```yaml
schedule:
  cron>: 0 2 * * *  # Daily at 2 AM UTC

_do:
  +run_pinterest_ingest:
    td_run>: pinterest_ingest
```

---

## Remember

✅ **DO:**
- Copy templates exactly as shown
- Define all CSV columns accurately
- Use secrets for credentials and file paths
- Test with small files first
- Monitor ingestion_log table

❌ **DON'T:**
- Change parser settings without understanding impact
- Hardcode credentials in configs
- Assume column order/names
- Skip testing before production

---

**When in doubt, copy the template exactly. These templates are production-tested.**
