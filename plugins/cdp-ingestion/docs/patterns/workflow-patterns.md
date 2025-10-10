# Workflow Patterns - Common Structures

## Overview
This document defines the exact, reproducible workflow patterns used across all ingestion sources. These patterns MUST be followed exactly to ensure production-ready code generation.

## Core Workflow Structure

All ingestion workflows follow this exact structure:

```yaml
timezone: UTC

_export:
  !include : config/database.yml
  !include : config/[source]_datasources.yml
  !include : config/hist_date_ranges.yml  # Only for historical workflows

+load_[incremental|historical]_data:
  _parallel: [true|{limit: N}]  # See parallel processing patterns below
  for_each>:
    datasource: ${[inc|hist]_datasources}
  _do:
    +log_ingestion_start:
      [Standard logging - see logging-patterns.md]

    +setup_table_and_time:
      [Table creation and time range setup - see below]

    +load_[incremental|batch]:
      [Data loading - see below]

    +log_ingestion_success:
      [Standard logging - see logging-patterns.md]

_error:
  +log_ingestion_error:
    [Error logging - see logging-patterns.md]
```

## Parallel Processing Patterns

### Pattern 1: Unlimited Parallel (BigQuery Only)
```yaml
+process_datasources:
  for_each>:
    datasource: ${datasources}
  _parallel: true  # No limit
  _do:
    [tasks...]
```

**Use When**: Data warehouse imports with no API rate limits

### Pattern 2: Limited Parallel (Most API Sources)
```yaml
+load_data:
  _parallel:
    limit: 3  # Maximum 3 concurrent jobs
  for_each>:
    datasource: ${datasources}
  _do:
    [tasks...]
```

**Use When**: API sources with rate limits (Klaviyo, OneTrust, Shopify)

### Pattern 3: Sequential (No Parallel)
```yaml
+load_data:
  for_each>:
    datasource: ${datasources}
  _do:
    [tasks...]
```

**Use When**: Strict ordering required or very restrictive rate limits

## Table Creation and Time Range Patterns

### Pattern 1: With Incremental Field
```yaml
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
            (SELECT [TIME_CONVERSION_FUNCTION] as start_time
              FROM ${databases.src}.${datasource.table_name}
              WHERE ${datasource.incremental_field} IS NOT NULL),
            '${datasource.default_start_time}'
          ) as start_time,
          [END_TIME_FUNCTION] as end_time
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
          [END_TIME_FUNCTION] as end_time
      database: ${databases.src}
      store_last_results: true
```

**Critical Variables**:
- `[TIME_CONVERSION_FUNCTION]`: See timestamp-formats.md for exact function per source
- `[END_TIME_FUNCTION]`: See timestamp-formats.md for exact function per source

### Pattern 2: Without Incremental Field (Full Load)
```yaml
+setup_table_and_time:
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
        '' as start_time,
        [END_TIME_FUNCTION] as end_time
    database: ${databases.src}
    store_last_results: true
```

## Data Loading Patterns

### Pattern 1: Standard Load
```yaml
+load_incremental:
  td_load>: config/${datasource.config_file}
  database: ${databases.src}
  table: ${datasource.table_name}
```

### Pattern 2: Load with Mode Override
```yaml
+load_data:
  td_load>: config/${datasource.config_file}
  database: ${databases.src}
  table: ${datasource.table_name}
  mode: ${datasource.mode}  # append or replace
```

## Dual-Mode Workflow Pattern (Historical + Incremental)

For sources supporting both historical and incremental modes:

```yaml
timezone: UTC

_export:
  !include : config/database.yml
  !include : config/[source]_datasources.yml
  !include : config/hist_date_ranges.yml
  run_mode: inc  # Default to incremental

+load_data:
  if>: ${run_mode == 'hist'}
  _do:
    +load_historical_data:
      [Historical workflow - see monthly batch pattern below]
  _else_do:
    +load_incremental_data:
      [Incremental workflow - see standard pattern above]

_error:
  +log_ingestion_error:
    [Error logging]
```

## Monthly Batch Processing Pattern (Historical Workflows)

For historical data processing with skip logic:

```yaml
+load_historical_data:
  _parallel: true
  for_each>:
    datasource: ${hist_datasources}
  _do:
    +log_ingestion_start:
      [Standard logging]

    +setup_table:
      [Table creation only, no time setup]

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
                   source_name = '[source]_${date_range.month_name}'
                   AND workflow_name = '[source]_ingest'
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
              [Logging with source_name = '[source]_${date_range.month_name}']

            +load_batch:
              td_load>: config/${datasource.config_file}
              database: ${databases.src}
              table: ${datasource.table_name}

            +log_batch_success:
              [Logging with source_name = '[source]_${date_range.month_name}']
          _else_do:
            +skip_batch:
              echo>: "Skipping ${date_range.month_name} for ${datasource.table_name} - already processed successfully"

    +log_ingestion_success:
      [Standard logging with overall source_name]
```

## Error Handling Pattern

All workflows MUST include this exact error handler:

```yaml
_error:
  +log_ingestion_error:
    td>: sql/log_ingestion_error.sql
    database: ${databases.src}
    source_name: '[source_name]'
    workflow_name: '[workflow_name]'
    error_message: '[descriptive error message]'
```

## Configuration File Includes

All workflows MUST include at minimum:

```yaml
_export:
  !include : config/database.yml
  !include : config/[source]_datasources.yml
```

Historical workflows additionally include:

```yaml
  !include : config/hist_date_ranges.yml
```

## Core Configuration Files

### database.yml (REQUIRED FOR ALL WORKFLOWS)

**File**: `config/database.yml`

**EXACT TEMPLATE**:
```yaml
databases:
  src: mck_src
  stg: mck_stg
  gld: mck_gld
  unif: cdp_unification_mck

td_authentication_ids:
  klaviyo:
    default: 360030
  google_bq:
    default: 360071
  shopify_v1:
    mccormick_flavors: 360617  # Update this if different from shopify_v2
  shopify_v2:
    default: 360360
  onetrust:
    default: 123  # Assuming same as klaviyo, update if different
  marketo:
    default: 123  # Assuming same as klaviyo, update if different
  pinterest:
    default: 123  # Assuming same as klaviyo, update if different
```

**CRITICAL**:
- Database names are referenced as `${databases.src}`, `${databases.stg}`, etc.
- Authentication IDs are centralized and referenced as `${td_authentication_ids.[source].default}`
- This replaces hardcoded authentication IDs in individual config files

### hist_date_ranges.yml (FOR HISTORICAL WORKFLOWS)

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
  # ... continue with additional monthly ranges as needed
```

**Note**: Extend this file with additional months as needed for historical backfill ranges

## Naming Conventions

### Workflow Files
- Incremental: `[source]_ingest_inc.dig`
- Historical: `[source]_ingest_hist.dig`
- Dual-mode: `[source]_ingest.dig`

### Workflow Task Names
- `+load_incremental_data` - For incremental workflows
- `+load_historical_data` - For historical workflows
- `+process_[source]_datasources` - For source-specific naming
- `+log_ingestion_start` - Always this name
- `+log_ingestion_success` - Always this name
- `+log_ingestion_error` - Always this name
- `+setup_table_and_time` - For incremental table setup
- `+setup_table` - For historical (no time setup)

## Critical Reminders

1. **ALWAYS** use `timezone: UTC`
2. **ALWAYS** include error handler with logging
3. **ALWAYS** use `CREATE TABLE IF NOT EXISTS`
4. **ALWAYS** use `store_last_results: true` for time queries
5. **ALWAYS** check `${datasource.incremental_field != "" && datasource.incremental_field != null}`
6. **NEVER** hardcode database names - use `${databases.src}`
7. **NEVER** skip logging - start, success, and error
