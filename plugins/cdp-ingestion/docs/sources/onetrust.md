# OneTrust Ingestion - Exact Templates

## Overview
OneTrust connector handles consent and privacy management data including data subject profiles and collection points. This source features a dual-mode workflow that supports both historical batch processing and incremental updates.

## Source Characteristics

| Characteristic | Value |
|---------------|-------|
| **Connector Type** | `onetrust` |
| **Parallel Processing** | Limited (`_parallel: {limit: 3}`) |
| **Timestamp Format** | `.000Z` (3 decimals, WITH Z) |
| **Workflow Mode** | Dual (historical + incremental) |
| **Historical Processing** | Monthly batches with skip logic |
| **Success Logging** | `log_ingestion_success.sql` (with time tracking) |
| **Data Sources** | Data Subject Profile, Collection Point |

---

## Dual-Mode Workflow

**File**: `onetrust_ingest.dig`

**EXACT TEMPLATE**:
```yaml
timezone: UTC

_export:
  !include : config/database.yml
  !include : config/onetrust_datasources.yml
  !include : config/hist_date_ranges.yml
  run_mode: inc


+load_data:
  if>: ${run_mode == 'hist'}
  _do:
    +load_historical_data:
      _parallel: true
      for_each>:
        datasource: ${hist_datasources}
      _do:
        +log_ingestion_start:
          td>: sql/log_ingestion_start.sql
          database: ${databases.src}
          source_name: 'onetrust'
          workflow_name: 'onetrust_ingest'
          table_name: ${datasource.table_name}

        +setup_table:
          if>: ${datasource.incremental_field != "" && datasource.incremental_field != null}
          _do:
            +create_incremental_table:
              td>:
              query: |
                CREATE TABLE IF NOT EXISTS ${databases.src}.${datasource.table_name} (
                  ${datasource.incremental_field} VARCHAR,
                  time BIGINT
                )
              database: ${databases.src}
          _else_do:
            +create_basic_table:
              td>:
              query: |
                CREATE TABLE IF NOT EXISTS ${databases.src}.${datasource.table_name} (
                  time BIGINT
                )
              database: ${databases.src}

        +load_monthly_batches:
          for_each>:
            date_range: ${date_ranges}
          _parallel:
            limit: 3
          _do:
            +check_batch_status:
              td>:
              query: |
                SELECT
                  COALESCE(
                    (SELECT COUNT(*)
                     FROM ${databases.src}.ingestion_log
                     WHERE
                       source_name = 'onetrust_${date_range.month_name}'
                       AND workflow_name = 'onetrust_ingest'
                       AND table_name = '${datasource.table_name}'
                       AND status = 'SUCCESS'),
                    0
                  ) as already_processed
              database: ${databases.src}
              store_last_results: true

            +process_batch:
              if>: ${td.last_results.already_processed == 0}
              _do:
                +set_batch_time_range:
                  td>:
                  query: |
                    SELECT
                      '${date_range.start_time}' as start_time,
                      '${date_range.end_time}' as end_time
                  database: ${databases.src}
                  store_last_results: true

                +log_batch_start:
                  td>: sql/log_ingestion_start.sql
                  database: ${databases.src}
                  source_name: 'onetrust_${date_range.month_name}'
                  workflow_name: 'onetrust_ingest'
                  table_name: ${datasource.table_name}

                +load_batch:
                  td_load>: config/${datasource.config_file}
                  database: ${databases.src}
                  table: ${datasource.table_name}

                +log_batch_success:
                  td>: sql/log_ingestion_success.sql
                  database: ${databases.src}
                  source_name: 'onetrust_${date_range.month_name}'
                  workflow_name: 'onetrust_ingest'
                  table_name: ${datasource.table_name}
              _else_do:
                +skip_batch:
                  echo>: "Skipping ${date_range.month_name} for ${datasource.table_name} - already processed successfully"

        +log_ingestion_success:
          td>: sql/log_ingestion_success.sql
          database: ${databases.src}
          source_name: 'onetrust'
          workflow_name: 'onetrust_ingest'
          table_name: ${datasource.table_name}

  _else_do:
    +load_incremental_data:
      _parallel:
        limit: 3
      for_each>:
        datasource: ${inc_datasources}
      _do:
        +log_ingestion_start:
          td>: sql/log_ingestion_start.sql
          database: ${databases.src}
          source_name: '${datasource.table_name}'
          workflow_name: '${datasource.workflow_name}'
          table_name: ${datasource.table_name}

        +setup_table_and_time:
          if>: ${datasource.incremental_field != "" && datasource.incremental_field != null}
          _do:
            +create_incremental_table:
              td>:
              query: |
                CREATE TABLE IF NOT EXISTS ${databases.src}.${datasource.table_name} (
                  ${datasource.incremental_field} VARCHAR,
                  time BIGINT
                )
              database: ${databases.src}

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
          _else_do:
            +create_basic_table:
              td>:
              query: |
                CREATE TABLE IF NOT EXISTS ${databases.src}.${datasource.table_name} (
                  time BIGINT
                )
              database: ${databases.src}

            +set_default_time:
              td>:
              query: |
                SELECT
                  '${datasource.default_start_time}' as start_time,
                  replace(to_iso8601(current_timestamp), 'Z', '.000Z') as end_time
              database: ${databases.src}
              store_last_results: true

        +load_incremental:
          td_load>: config/${datasource.config_file}
          database: ${databases.src}
          table: ${datasource.table_name}

        +log_ingestion_success:
          td>: sql/log_ingestion_success.sql
          database: ${databases.src}
          source_name: '${datasource.table_name}'
          workflow_name: '${datasource.workflow_name}'
          table_name: ${datasource.table_name}

_error:
  +log_ingestion_error:
    td>: sql/log_ingestion_error.sql
    database: ${databases.src}
    source_name: 'onetrust'
    workflow_name: 'onetrust_ingest'
    error_message: 'onetrust ingestion failed. check onetrust_ingest'
```

---

## Datasources Configuration

**File**: `config/onetrust_datasources.yml`

**EXACT TEMPLATE**:
```yaml
hist_datasources:
  - name: data_subject_profile
    table_name: onetrust_data_subject_profile
    config_file: onetrust_data_subject_profile_load.yml
    incremental_field: updated_date
    default_start_time: "2025-01-30T00:49:04Z"
    workflow_name: onetrust_ingest.dig
    mode: append

  - name: collection_point
    table_name: onetrust_collection_point
    config_file: onetrust_collection_point_load.yml
    incremental_field: updated_date
    default_start_time: "2025-01-30T00:49:04Z"
    workflow_name: onetrust_ingest.dig
    mode: append

inc_datasources:
  - name: data_subject_profile
    table_name: onetrust_data_subject_profile
    config_file: onetrust_data_subject_profile_load.yml
    incremental_field: updated_date
    default_start_time: "2025-01-30T00:49:04Z"
    workflow_name: onetrust_ingest.dig
    mode: append

  - name: collection_point
    table_name: onetrust_collection_point
    config_file: onetrust_collection_point_load.yml
    incremental_field: updated_date
    default_start_time: "2025-01-30T00:49:04Z"
    workflow_name: onetrust_ingest.dig
    mode: append
```

**CRITICAL**: OneTrust uses `mode: append` (NOT replace)

---

## Load Configuration Files

### Data Subject Profile

**File**: `config/onetrust_data_subject_profile_load.yml`

**EXACT TEMPLATE**:
```yaml
in:
  type: onetrust
  base_url: ${secret:onetrust_base_url}
  auth_method: oauth
  access_token: ${secret:onetrust_access_token}
  data_type: data_subject_profile
  incremental: true
  start_time: ${td.last_results.start_time}
  end_time: ${td.last_results.end_time}
  incremental_field: updated_date
  thread_count: 5

filters:
- type: add_time
  to_column:
    name: time
    type: timestamp
  from_value:
    mode: upload_time

out:
  mode: ${datasource.mode}
```

### Collection Point

**File**: `config/onetrust_collection_point_load.yml`

**EXACT TEMPLATE**:
```yaml
in:
  type: onetrust
  base_url: ${secret:onetrust_base_url}
  auth_method: oauth
  access_token: ${secret:onetrust_access_token}
  data_type: collection_point
  incremental: true
  start_time: ${td.last_results.start_time}
  end_time: ${td.last_results.end_time}
  incremental_field: updated_date
  thread_count: 5

filters:
- type: add_time
  to_column:
    name: time
    type: timestamp
  from_value:
    mode: upload_time

out:
  mode: ${datasource.mode}
```

---

## Critical OneTrust Patterns

### 1. Dual-Mode Operation

**Default Mode**: Incremental (`run_mode: inc`)

**Switch to Historical**:
```yaml
_export:
  run_mode: hist  # Change to 'hist' for historical backfill
```

**Mode Selection Logic**:
```yaml
+load_data:
  if>: ${run_mode == 'hist'}
  _do:
    [Historical workflow with monthly batches]
  _else_do:
    [Incremental workflow]
```

---

### 2. Monthly Batch Processing (Historical Mode)

**Pattern**:
- Processes data in monthly chunks defined in `config/hist_date_ranges.yml`
- Checks ingestion_log before processing each month
- Skips months already successfully processed
- Uses date-specific source names for tracking

**Batch Status Check**:
```sql
SELECT COALESCE(
  (SELECT COUNT(*)
   FROM ${databases.src}.ingestion_log
   WHERE
     source_name = 'onetrust_${date_range.month_name}'
     AND workflow_name = 'onetrust_ingest'
     AND table_name = '${datasource.table_name}'
     AND status = 'SUCCESS'),
  0
) as already_processed
```

**Date-Specific Logging**:
```yaml
source_name: 'onetrust_${date_range.month_name}'  # e.g., "onetrust_2023-09"
```

---

### 3. OneTrust Timestamp Functions

**Start Time** (Get Max from Table):
```sql
replace(to_iso8601(TD_TIME_PARSE(max(${datasource.incremental_field}))), 'Z', '.000Z')
```

**End Time** (Current Timestamp):
```sql
replace(to_iso8601(current_timestamp), 'Z', '.000Z')
```

**Format**: `.000Z` (3 decimals, WITH Z suffix)

**Example Output**:
- Start: `2025-01-30T00:49:04.000Z`
- End: `2025-10-10T13:45:30.000Z`

---

### 4. Authentication

**Required Secrets** (in `credentials_ingestion.json`):
```json
{
  "onetrust_base_url": "https://api.onetrust.com",
  "onetrust_access_token": "your_access_token_here"
}
```

**In Load Config**:
```yaml
base_url: ${secret:onetrust_base_url}
auth_method: oauth
access_token: ${secret:onetrust_access_token}
```

---

### 5. Thread Count

```yaml
thread_count: 5
```

OneTrust connector supports parallel API requests. Setting to 5 balances performance with API rate limits.

---

## Historical Date Ranges Configuration

**File**: `config/hist_date_ranges.yml`

**EXACT TEMPLATE**:
```yaml
date_ranges:
  - start_time: "2023-09-01T00:00:00.000000"
    end_time: "2023-09-30T23:59:59.000000"
    month_name: "2023-09"
  - start_time: "2023-10-01T00:00:00.000000"
    end_time: "2023-10-31T23:59:59.000000"
    month_name: "2023-10"
  - start_time: "2023-11-01T00:00:00.000000"
    end_time: "2023-11-30T23:59:59.000000"
    month_name: "2023-11"
  # Add more monthly ranges as needed
```

**Pattern**:
- Each month is a separate range
- `month_name` used in logging and skip logic
- Extend as needed for your date range requirements

---

## Running OneTrust Workflows

### Incremental Mode (Default)
```bash
td wf run onetrust_ingest.dig
```

### Historical Mode
Edit `onetrust_ingest.dig`:
```yaml
_export:
  run_mode: hist  # Change from 'inc' to 'hist'
```

Then run:
```bash
td wf run onetrust_ingest.dig
```

---

## Adding New OneTrust Data Types

### Step 1: Verify Data Type Name

Check OneTrust connector documentation for supported data types.

**Current types**:
- `data_subject_profile`
- `collection_point`

### Step 2: Add to Datasources Config

Add to both `hist_datasources` and `inc_datasources` in `config/onetrust_datasources.yml`:

```yaml
hist_datasources:
  - name: [data_type_name]
    table_name: onetrust_[data_type_name]
    config_file: onetrust_[data_type_name]_load.yml
    incremental_field: updated_date
    default_start_time: "2025-01-30T00:49:04Z"
    workflow_name: onetrust_ingest.dig
    mode: append

inc_datasources:
  - name: [data_type_name]
    table_name: onetrust_[data_type_name]
    config_file: onetrust_[data_type_name]_load.yml
    incremental_field: updated_date
    default_start_time: "2025-01-30T00:49:04Z"
    workflow_name: onetrust_ingest.dig
    mode: append
```

### Step 3: Create Load Config

Create `config/onetrust_[data_type_name]_load.yml`:

```yaml
in:
  type: onetrust
  base_url: ${secret:onetrust_base_url}
  auth_method: oauth
  access_token: ${secret:onetrust_access_token}
  data_type: [data_type_name]
  incremental: true
  start_time: ${td.last_results.start_time}
  end_time: ${td.last_results.end_time}
  incremental_field: updated_date
  thread_count: 5

filters:
- type: add_time
  to_column:
    name: time
    type: timestamp
  from_value:
    mode: upload_time

out:
  mode: ${datasource.mode}
```

### Step 4: Test

```bash
td wf run onetrust_ingest.dig
```

---

## Common OneTrust Issues

### Issue: "401 Unauthorized"
**Cause**: Invalid or expired access token

**Solution**:
1. Verify `onetrust_access_token` in credentials
2. Check token hasn't expired
3. Ensure auth_method is set to `oauth`

### Issue: "Invalid timestamp format"
**Cause**: Missing `.000Z` suffix or wrong decimal count

**Solution**:
1. Verify using `.000Z` (3 decimals, WITH Z)
2. Use exact timestamp functions from this document

### Issue: Historical batches re-processing
**Cause**: Logging not capturing batch completion correctly

**Solution**:
1. Verify `source_name: 'onetrust_${date_range.month_name}'` in batch logging
2. Check ingestion_log table for SUCCESS entries
3. Ensure month_name matches exactly

### Issue: "Thread count error"
**Cause**: Missing or invalid thread_count parameter

**Solution**: Add `thread_count: 5` to load config

### Issue: No data in incremental mode
**Cause**: start_time > end_time or no updates in timeframe

**Solution**:
1. Check default_start_time is not in future
2. Verify data actually has updated_date field populated
3. Test with historical mode first

---

## Performance Optimization

### 1. Use Appropriate Parallel Limits

**Historical**: `_parallel: true` (no limit for datasources, limit: 3 for batches)
**Incremental**: `_parallel: {limit: 3}`

### 2. Thread Count

```yaml
thread_count: 5  # Balances performance with API limits
```

### 3. Skip Logic

Historical mode automatically skips completed batches - no manual intervention needed.

### 4. Monthly Batches

Processes large historical datasets in manageable chunks, preventing timeouts.

---

## Critical Reminders

1. **ALWAYS use `.000Z` timestamp format** (3 decimals, WITH Z)
2. **ALWAYS use `mode: append`** (NOT replace) for OneTrust
3. **ALWAYS set `run_mode: inc`** as default (change to `hist` only for backfill)
4. **REMEMBER**: Historical mode uses date-specific source names (`onetrust_2023-09`)
5. **REMEMBER**: Skip logic checks ingestion_log before processing each month
6. **USE `thread_count: 5`** for optimal performance
7. **USE `auth_method: oauth`** with access_token
8. **VERIFY** base_url and access_token are in secrets
9. **EXTEND** hist_date_ranges.yml as needed for your date range
10. **TEST** with incremental mode first before running historical backfill

---

## Data Type Reference

| Data Type | Incremental Field | Thread Count | Special Notes |
|-----------|------------------|--------------|---------------|
| data_subject_profile | updated_date | 5 | Profile updates |
| collection_point | updated_date | 5 | Consent collection points |

---

## Reference

**Related Documentation**:
- Timestamp formats: `docs/patterns/timestamp-formats.md`
- Workflow patterns: `docs/patterns/workflow-patterns.md`
- Logging patterns: `docs/patterns/logging-patterns.md`
- Incremental patterns: `docs/patterns/incremental-patterns.md`

**Connector Documentation**:
- [Treasure Data OneTrust Connector](https://docs.treasuredata.com/display/public/PD/OneTrust+Import+Integration)
