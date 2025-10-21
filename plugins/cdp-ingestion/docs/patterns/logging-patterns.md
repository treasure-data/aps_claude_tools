# Logging Patterns - SQL Templates

## Overview
All ingestion workflows MUST use these exact SQL logging templates. These templates ensure consistent monitoring and debugging across all data sources.

## Ingestion Log Table Schema

The ingestion_log table MUST be created with this exact schema:

```sql
CREATE TABLE IF NOT EXISTS ${databases.src}.ingestion_log (
    log_id VARCHAR,
    source_name VARCHAR,
    workflow_name VARCHAR,
    table_name VARCHAR,
    status VARCHAR,
    start_time VARCHAR,
    end_time VARCHAR,
    records_processed BIGINT,
    error_message VARCHAR,
    job_id VARCHAR,
    job_url VARCHAR,
    time BIGINT
)
```

**File**: `sql/create_ingestion_log_table.sql`

## Logging SQL Templates

### 1. Log Ingestion Start

**File**: `sql/log_ingestion_start.sql`

**Exact Template**:
```sql
INSERT INTO ${databases.src}.ingestion_log
SELECT
    concat('${source_name}_${table_name}_', cast(CURRENT_TIMESTAMP as varchar)) as log_id,
    '${source_name}' as source_name,
    '${workflow_name}' as workflow_name,
    '${table_name}' as table_name,
    'STARTED' as status,
    cast(now() as varchar) as start_time,
    null as end_time,
    0 as records_processed,
    null as error_message,
    null as job_id,
    null as job_url,
    cast(floor(to_unixtime(now())) as bigint) as time
```

**Usage in Workflow**:
```yaml
+log_ingestion_start:
  td>: sql/log_ingestion_start.sql
  database: ${databases.src}
  source_name: '[source_identifier]'
  workflow_name: '[workflow_file_name]'
  table_name: ${datasource.table_name}
```

**When to Use**: At the beginning of EVERY data source ingestion task

---

### 2. Log Ingestion Success (Standard)

**File**: `sql/log_ingestion_success.sql`

**Exact Template**:
```sql
INSERT INTO ${databases.src}.ingestion_log
SELECT
    concat('${source_name}_${table_name}_', cast(CURRENT_TIMESTAMP as varchar)) as log_id,
    '${source_name}' as source_name,
    '${workflow_name}' as workflow_name,
    '${table_name}' as table_name,
    'SUCCESS' as status,
    '${td.last_results.start_time}' as start_time,
    '${td.last_results.end_time}' as end_time,
    ${td.last_job.num_records} as records_processed,
    null as error_message,
    '${td.last_job.id}' as job_id,
    'https://console.treasuredata.com/app/jobs/${td.last_job.id}/output-logs' as job_url,
    cast(floor(to_unixtime(now())) as bigint) as time
```

**Usage in Workflow**:
```yaml
+log_ingestion_success:
  td>: sql/log_ingestion_success.sql
  database: ${databases.src}
  source_name: '[source_identifier]'
  workflow_name: '[workflow_file_name]'
  table_name: ${datasource.table_name}
```

**When to Use**: After successful td_load when start_time/end_time are tracked in td.last_results

**Used By**: Klaviyo, OneTrust, Shopify workflows

---

### 3. Log Ingestion Success (Alternative - No Time Tracking)

**File**: `sql/log_ingestion_success_2.sql`

**Exact Template**:
```sql
INSERT INTO ${databases.src}.ingestion_log
SELECT
    concat('${source_name}_${table_name}_', cast(CURRENT_TIMESTAMP as varchar)) as log_id,
    '${source_name}' as source_name,
    '${workflow_name}' as workflow_name,
    '${table_name}' as table_name,
    'SUCCESS' as status,
    '' as start_time,
    '' as end_time,
    ${td.last_job.num_records} as records_processed,
    null as error_message,
    '${td.last_job.id}' as job_id,
    'https://console.treasuredata.com/app/jobs/${td.last_job.id}/output-logs' as job_url,
    cast(floor(to_unixtime(now())) as bigint) as time
```

**Usage in Workflow**:
```yaml
+log_ingestion_success:
  td>: sql/log_ingestion_success_2.sql
  database: ${databases.src}
  source_name: '[source_identifier]'
  workflow_name: '[workflow_file_name]'
  table_name: ${datasource.table_name}
```

**When to Use**: After successful td_load when start_time/end_time are NOT tracked in td.last_results

**Used By**: Google BigQuery workflows

---

### 4. Log Ingestion Error

**File**: `sql/log_ingestion_error.sql`

**Exact Template**:
```sql
INSERT INTO ${databases.src}.ingestion_log
SELECT
    concat('${source_name}_', cast(CURRENT_TIMESTAMP as varchar)) as log_id,
    '${source_name}' as source_name,
    '${workflow_name}' as workflow_name,
    null as table_name,
    'ERROR' as status,
    null as start_time,
    cast(now() as varchar) as end_time,
    0 as records_processed,
    '${error_message}' as error_message,
    null as job_id,
    null as job_url,
    cast(floor(to_unixtime(now())) as bigint) as time
```

**Usage in Workflow**:
```yaml
_error:
  +log_ingestion_error:
    td>: sql/log_ingestion_error.sql
    database: ${databases.src}
    source_name: '[source_name]'
    workflow_name: '[workflow_name]'
    error_message: '[descriptive error message]'
```

**When to Use**: In _error blocks to log workflow failures

---

### 5. Log Runner Completion

**File**: `sql/log_runner_completion.sql`

**Exact Template**:
```sql
INSERT INTO ${databases.src}.ingestion_log (
    log_id,
    source_name,
    workflow_name,
    status,
    start_time,
    end_time,
    records_processed,
    error_message,
    time
)
VALUES (
    concat('ingestion_runner_', cast(floor(to_unixtime(now())) as varchar)),
    'ingestion_runner',
    'ingestion_runner',
    'COMPLETED',
    null,
    cast(now() as varchar),
    ${total_workflows},
    null,
    floor(to_unixtime(now()))
)
```

**Usage in Workflow**:
```yaml
+log_runner_completion:
  td>: sql/log_runner_completion.sql
  database: ${databases.src}
  total_workflows: [number_of_workflows_run]
```

**When to Use**: ONLY in ingestion_runner.dig orchestrator workflow

---

### 6. Log Runner Error

**File**: `sql/log_runner_error.sql`

**Exact Template**:
```sql
INSERT INTO ${databases.src}.ingestion_log (
    log_id,
    source_name,
    workflow_name,
    status,
    start_time,
    end_time,
    records_processed,
    error_message,
    time
)
VALUES (
    concat('ingestion_runner_error_', cast(floor(to_unixtime(now())) as varchar)),
    'ingestion_runner',
    'ingestion_runner',
    'ERROR',
    null,
    cast(now() as varchar),
    0,
    '${error_message}',
    floor(to_unixtime(now()))
)
```

**Usage in Workflow**:
```yaml
_error:
  +log_runner_error:
    td>: sql/log_runner_error.sql
    database: ${databases.src}
    error_message: '${error.message}'
```

**When to Use**: ONLY in ingestion_runner.dig _error block

---

## SQL Template Parameters Reference

| Parameter | Description | Example Value | Required |
|-----------|-------------|---------------|----------|
| `${databases.src}` | Source database name | `client_src` | Yes |
| `${source_name}` | Data source identifier | `klaviyo`, `shopify`, `onetrust` | Yes |
| `${workflow_name}` | Workflow file name | `klaviyo_ingest.dig` | Yes |
| `${table_name}` | Target table name | `klaviyo_events` | Yes (except error) |
| `${td.last_results.start_time}` | Start time from last query | `2023-09-01T00:00:00.000000` | Only for success |
| `${td.last_results.end_time}` | End time from last query | `2023-09-30T23:59:59.000000` | Only for success |
| `${td.last_job.num_records}` | Records processed in last job | `1500` | Only for success |
| `${td.last_job.id}` | Treasure Data job ID | `12345678` | Only for success |
| `${error.message}` | Error message from failed task | `Connection timeout` | Only for error |
| `${total_workflows}` | Total workflows run | `5` | Only for runner |

---

## Logging Decision Tree

```
Is this the ingestion_runner workflow?
├─ YES: Use log_runner_completion.sql or log_runner_error.sql
└─ NO: Continue...

    Is this the start of a data source task?
    ├─ YES: Use log_ingestion_start.sql
    └─ NO: Continue...

        Did the task complete successfully?
        ├─ YES: Does td.last_results have start_time/end_time?
        │   ├─ YES: Use log_ingestion_success.sql
        │   └─ NO: Use log_ingestion_success_2.sql
        └─ NO: Use log_ingestion_error.sql (in _error block)
```

---

## Special Logging for Monthly Batches

For historical workflows with monthly batch processing, use date-specific source names:

**Pattern**:
```yaml
source_name: '[source]_${date_range.month_name}'
```

**Example**:
```yaml
+log_batch_start:
  td>: sql/log_ingestion_start.sql
  database: ${databases.src}
  source_name: 'onetrust_${date_range.month_name}'  # e.g., "onetrust_2023-09"
  workflow_name: 'onetrust_ingest'
  table_name: ${datasource.table_name}
```

This allows tracking each monthly batch separately in the ingestion_log table.

---

## Utility SQL Templates

### Get Last Record Time for BigQuery (Parameterized)

**File**: `sql/get_last_record_time_bigquery.sql`

**Exact Template**:
```sql
SELECT
  COALESCE(
    MAX(updated_at),
    '1970-01-01 00:00:00'
  ) AS last_record_time
FROM ${databases.src}.${table_name}
WHERE time >= FLOOR(TO_UNIXTIME(NOW()) - 86400 * 30)
LIMIT 1
```

**Usage**: Get the last record time for a specific BigQuery table (looks at last 30 days)

---

### Get Last Record Time for BigQuery (Hardcoded Table)

**File**: `sql/get_last_record_time_bigquery_query.sql`

**Exact Template**:
```sql
SELECT
  COALESCE(
    MAX(updated_at),
    '1970-01-01 00:00:00'
  ) AS last_record_time
FROM ${databases.src}.google_bigquery_query_data
WHERE time >= FLOOR(TO_UNIXTIME(NOW()) - 86400 * 30)
LIMIT 1
```

**Usage**: Get the last record time specifically for google_bigquery_query_data table

---

## Critical Reminders

1. **ALWAYS** log start before any data processing
2. **ALWAYS** log success after successful td_load
3. **ALWAYS** include _error block with error logging
4. **NEVER** skip logging steps
5. **NEVER** modify SQL template structure - only change parameters
6. **USE** source_name consistently across start/success/error for same task
7. **USE** date-specific source_name for monthly batches (e.g., 'onetrust_2023-09')
8. **VERIFY** td.last_results.start_time exists before using log_ingestion_success.sql
