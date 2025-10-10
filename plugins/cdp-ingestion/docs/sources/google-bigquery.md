# Google BigQuery Ingestion - Exact Templates

## Overview
Google BigQuery ingestion uses the BigQuery v2 connector to import data from Google Cloud Platform. This source supports both incremental query-based imports and full table imports, with separate workflows for historical backfill and incremental processing.

## Source Characteristics

| Characteristic | Value |
|---------------|----------|
| **Connector Type** | `bigquery_v2` |
| **Parallel Processing (Incremental)** | Limited (`_parallel: {limit: 3}`) |
| **Parallel Processing (Historical)** | Unlimited (`_parallel: true`) |
| **Timestamp Format** | SQL Server style (`yyyy-MM-dd HH:mm:ss.SSS`) |
| **Incremental Parameter** | `inc_field` (NOT `incremental_field`) |
| **Import Types** | `query` (with WHERE clause) or `table` (full import) |
| **Success Logging** | `log_ingestion_success.sql` (with time tracking) |
| **Workflows** | Separate hist/inc workflows |

---

## Workflow Files

### Incremental Workflow

**File**: `google_bigquery_ingest_inc.dig`

**EXACT TEMPLATE**:
```yaml
timezone: UTC

_export:
  !include : config/database.yml
  !include : config/google_bq_datasources.yml
  !include : config/hist_date_ranges.yml

+load_incremental_data:
  _parallel:
    limit: 3
  for_each>:
    datasource: ${inc_datasources}
  _do:
    +log_ingestion_start:
      td>: sql/log_ingestion_start.sql
      database: ${databases.src}
      source_name: 'google_bigquery'
      workflow_name: 'google_bigquery_ingest_inc'
      table_name: ${datasource.table}

    +setup_table_and_time:
      if>: ${datasource.inc_field != "" && datasource.inc_field != null}
      _do:
        +create_incremental_table:
          td>:
          query: |
            CREATE TABLE IF NOT EXISTS ${databases.src}.${datasource.table} (
              ${datasource.inc_field} VARCHAR,
              time BIGINT
            )
          database: ${databases.src}

        +get_last_incremental_time:
          td>:
          query: |
            SELECT
              COALESCE(
                (SELECT MAX(${datasource.inc_field}) as start_time
                  FROM ${databases.src}.${datasource.table}
                  WHERE ${datasource.inc_field} IS NOT NULL),
                '${datasource.default_start_time}'
              ) as start_time,
              FORMAT_DATETIME(CURRENT_TIMESTAMP, 'yyyy-MM-dd'' ''HH:mm:ss.SSS') as end_time
          database: ${databases.src}
          store_last_results: true
      _else_do:
        +create_basic_table:
          td>:
          query: |
            CREATE TABLE IF NOT EXISTS ${databases.src}.${datasource.table} (
              time BIGINT
            )
          database: ${databases.src}

        +set_default_time:
          td>:
          query: |
            SELECT
              '${datasource.default_start_time}' as start_time,
              FORMAT_DATETIME(CURRENT_TIMESTAMP, 'yyyy-MM-dd'' ''HH:mm:ss.SSS') as end_time
          database: ${databases.src}
          store_last_results: true

    +load_incremental:
      td_load>: config/${datasource.config_file}
      database: ${databases.src}
      table: ${datasource.table}
      mode: ${datasource.mode}

    +log_ingestion_success:
      td>: sql/log_ingestion_success.sql
      database: ${databases.src}
      source_name: 'google_bigquery'
      workflow_name: 'google_bigquery_ingest_inc'
      table_name: ${datasource.table}

_error:
  +log_ingestion_error:
    td>: sql/log_ingestion_error.sql
    database: ${databases.src}
    source_name: 'google_bigquery'
    workflow_name: 'google_bigquery_ingest_inc'
    error_message: 'google bigquery incremental ingestion failed. check google_bigquery_ingest_inc'
```

### Historical Workflow

**File**: `google_bigquery_ingest_hist.dig`

**EXACT TEMPLATE**:
```yaml
timezone: UTC

_export:
  !include : config/database.yml
  !include : config/google_bq_datasources.yml
  !include : config/hist_date_ranges.yml

+load_historical_data:
  _parallel: true
  for_each>:
    datasource: ${hist_datasources}
  _do:
    +log_ingestion_start:
      td>: sql/log_ingestion_start.sql
      database: ${databases.src}
      source_name: 'google_bigquery'
      workflow_name: 'google_bigquery_ingest_hist'
      table_name: ${datasource.table}

    +setup_table:
      if>: ${datasource.inc_field != "" && datasource.inc_field != null}
      _do:
        +create_incremental_table:
          td>:
          query: |
            CREATE TABLE IF NOT EXISTS ${databases.src}.${datasource.table} (
              ${datasource.inc_field} VARCHAR,
              time BIGINT
            )
          database: ${databases.src}
      _else_do:
        +create_basic_table:
          td>:
          query: |
            CREATE TABLE IF NOT EXISTS ${databases.src}.${datasource.table} (
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
                    source_name = 'google_bigquery_${date_range.month_name}'
                    AND workflow_name = 'google_bigquery_ingest_hist'
                    AND table_name = '${datasource.table}'
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
              source_name: 'google_bigquery_${date_range.month_name}'
              workflow_name: 'google_bigquery_ingest_hist'
              table_name: ${datasource.table}

            +load_batch:
              td_load>: config/${datasource.config_file}
              database: ${databases.src}
              table: ${datasource.table}
              mode: ${datasource.mode}

            +log_batch_success:
              td>: sql/log_ingestion_success.sql
              database: ${databases.src}
              source_name: 'google_bigquery_${date_range.month_name}'
              workflow_name: 'google_bigquery_ingest_hist'
              table_name: ${datasource.table}
          _else_do:
            +skip_batch:
              echo>: "Skipping ${date_range.month_name} for ${datasource.table} - already processed successfully"

    +log_ingestion_success:
      td>: sql/log_ingestion_success.sql
      database: ${databases.src}
      source_name: 'google_bigquery'
      workflow_name: 'google_bigquery_ingest_hist'
      table_name: ${datasource.table}

_error:
  +log_ingestion_error:
    td>: sql/log_ingestion_error.sql
    database: ${databases.src}
    source_name: 'google_bigquery'
    workflow_name: 'google_bigquery_ingest_hist'
    error_message: 'google bigquery historical ingestion failed. check google_bigquery_ingest_hist'
```

---

## Datasources Configuration

**File**: `config/google_bq_datasources.yml`

**EXACT TEMPLATE**:
```yaml
hist_datasources:
  - name: shopify_order_historical
    table: gbq_shopify_order_hist
    dataset: gold_treasure_data
    config_file: google_bq_shopify_order_historical.yml
    workflow_name: google_bigquery_ingest_hist.dig
    mode: append
    inc_field: incremental_date
    default_start_time: "2023-08-31 23:59:59.000"

  - name: shopify_transaction_historical
    table: gbq_shopify_transaction_hist
    dataset: gold_treasure_data
    config_file: google_bq_shopify_transaction_historical.yml
    workflow_name: google_bigquery_ingest_hist.dig
    mode: append
    inc_field: incremental_date
    default_start_time: "2023-08-31 23:59:59.000"

  - name: shopify_order_adjustments_historical
    table: gbq_shopify_order_adjustments_hist
    dataset: gold_treasure_data
    config_file: google_bq_shopify_order_adjustments_historical.yml
    workflow_name: google_bigquery_ingest_hist.dig
    mode: append
    inc_field: incremental_date
    default_start_time: "2023-08-31 23:59:59.000"

  - name: shopify_discount_allocation_historical
    table: gbq_shopify_discount_allocation_hist
    dataset: gold_treasure_data
    config_file: google_bq_shopify_discount_allocation_historical.yml
    workflow_name: google_bigquery_ingest_hist.dig
    mode: append
    inc_field: incremental_date
    default_start_time: "2023-08-31 23:59:59.000"

  - name: shopify_order_line_historical
    table: gbq_shopify_order_line_hist
    dataset: gold_treasure_data
    config_file: google_bq_shopify_order_line_historical.yml
    workflow_name: google_bigquery_ingest_hist.dig
    mode: append
    inc_field: incremental_date
    default_start_time: "2023-08-31 23:59:59.000"

  - name: gold_user_profile_historical
    table: gbq_user_profile_hist
    dataset: gold_treasure_data
    config_file: google_bq_gold_user_profile_load.yml
    workflow_name: google_bigquery_ingest_hist.dig
    mode: append
    inc_field:
    default_start_time: "2023-08-31 23:59:59.000"

inc_datasources:
  - name: shopify_order
    table: gbq_shopify_order
    dataset: gold_treasure_data
    config_file: google_bq_shopify_order.yml
    workflow_name: google_bigquery_ingest_inc.dig
    mode: append
    inc_field: incremental_date
    default_start_time: "2025-08-31 23:59:59.000"

  - name: shopify_transaction
    table: gbq_shopify_transaction
    dataset: gold_treasure_data
    config_file: google_bq_shopify_transaction.yml
    workflow_name: google_bigquery_ingest_inc.dig
    mode: append
    inc_field: incremental_date
    default_start_time: "2025-08-31 23:59:59.000"

  - name: shopify_order_adjustments
    table: gbq_shopify_order_adjustments
    dataset: gold_treasure_data
    config_file: google_bq_shopify_order_adjustments.yml
    workflow_name: google_bigquery_ingest_inc.dig
    mode: append
    inc_field: incremental_date
    default_start_time: "2025-08-31 23:59:59.000"

  - name: shopify_discount_allocation
    table: gbq_shopify_discount_allocation
    dataset: gold_treasure_data
    config_file: google_bq_shopify_discount_allocation.yml
    workflow_name: google_bigquery_ingest_inc.dig
    mode: append
    inc_field: incremental_date
    default_start_time: "2025-08-31 23:59:59.000"

  - name: shopify_order_line
    table: gbq_shopify_order_line
    dataset: gold_treasure_data
    config_file: google_bq_shopify_order_line.yml
    workflow_name: google_bigquery_ingest_inc.dig
    mode: append
    inc_field: incremental_date
    default_start_time: "2025-08-31 23:59:59.000"
```

**Required Fields**:
- `name`: BigQuery table name (used in SQL queries)
- `table`: Treasure Data table name
- `dataset`: BigQuery dataset name
- `config_file`: Load configuration file name
- `workflow_name`: Workflow file name
- `mode`: `append` or `replace`
- `inc_field`: Incremental field name (empty string for full loads)
- `default_start_time`: Default start time in SQL Server format

**CRITICAL**: No `td_authentication_id` at top level - auth ID now comes from `database.yml`

---

## Database Configuration (UPDATED)

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

**CRITICAL CHANGE**: Authentication IDs are now centralized in `database.yml` with hierarchical structure

---

## Load Configuration Files

### Type 1: Query-Based Import (With Incremental)

**File**: `config/google_bq_shopify_order.yml`

**EXACT TEMPLATE**:
```yaml
in:
  type: bigquery_v2
  td_authentication_id: ${td_authentication_ids.google_bq.default}
  project_id: prj-dgtl-p-data-ext-prvdr-3157
  dataset: ${datasource.dataset}
  import_type: query
  query: SELECT * FROM ${datasource.dataset}.${datasource.name} WHERE ${datasource.inc_field} > '${td.last_results.start_time}'
  export_to_gcs: false
  max_connection_retry: 10

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

**CRITICAL CHANGE**: Now uses `${td_authentication_ids.google_bq.default}` instead of hardcoded `360071`

**Key Parameters**:
- `import_type: query` - Enables custom SQL query
- `query:` - SQL with WHERE clause for incremental loading
- `${datasource.inc_field}` - Used in WHERE clause
- `${td.last_results.start_time}` - From previous workflow step

---

### Type 2: Table Import (Full Load, No Incremental) - Historical

**File**: `config/google_bq_shopify_order_historical.yml`

**EXACT TEMPLATE**:
```yaml
in:
  type: bigquery_v2
  td_authentication_id: ${td_authentication_ids.google_bq.default}
  project_id: prj-dgtl-p-data-ext-prvdr-3157
  import_type: table
  dataset: ${datasource.dataset}
  table: ${datasource.name}

  export_to_gcs: false
  max_connection_retry: 10

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

**Key Parameters**:
- `import_type: table` - Full table import
- `table:` - BigQuery table name
- No `query` parameter
- No incremental filtering (batches controlled by workflow date ranges)

---

## Critical BigQuery-Specific Patterns

### 1. Dual-Mode Workflows (Like Other Sources)

**CRITICAL CHANGE**: BigQuery now follows the same pattern as Klaviyo, OneTrust, and Shopify v2

```yaml
# Historical workflow
+load_historical_data:
  _parallel: true  # ← Unlimited for historical
  for_each>:
    datasource: ${hist_datasources}

# Incremental workflow
+load_incremental_data:
  _parallel:
    limit: 3  # ← Limited for incremental
  for_each>:
    datasource: ${inc_datasources}
```

**Why**:
- Historical: Unlimited parallel for fast backfill
- Incremental: Limited parallel to avoid overwhelming BigQuery API

---

### 2. Monthly Batch Processing for Historical

**CRITICAL PATTERN**: Historical workflow uses monthly batches with skip logic (like OneTrust)

```yaml
+load_monthly_batches:
  for_each>:
    date_range: ${date_ranges}
  _parallel:
    limit: 3
  _do:
    +check_batch_status:
      # Check if batch already processed

    +process_batch:
      if>: ${td.last_results.already_processed == 0}
      _do:
        # Log with date-specific source name
        source_name: 'google_bigquery_${date_range.month_name}'
```

**Why**: Allows resuming historical backfill without reprocessing completed months

---

### 3. Standard Success Logging (CHANGED)

**CRITICAL CHANGE**: Now uses `log_ingestion_success.sql` (NOT `log_ingestion_success_2.sql`)

```yaml
+log_ingestion_success:
  td>: sql/log_ingestion_success.sql  # ← Uses standard logging
  database: ${databases.src}
  source_name: 'google_bigquery'
  workflow_name: 'google_bigquery_ingest_inc'
  table_name: ${datasource.table}
```

**Why**: With new workflow structure, start_time/end_time are now tracked in td.last_results

---

### 4. SQL Server Timestamp Format

```sql
FORMAT_DATETIME(CURRENT_TIMESTAMP, 'yyyy-MM-dd'' ''HH:mm:ss.SSS')
```

**Output**: `2025-10-10 13:45:30.123`

**Note**: Uses space separator, NOT 'T'

---

### 5. inc_field Instead of incremental_field

```yaml
# In datasources config:
inc_field: incremental_date

# In workflow:
${datasource.inc_field}  # NOT incremental_field

# In load config:
WHERE ${datasource.inc_field} > '${td.last_results.start_time}'
```

**Critical**: Always use `inc_field` parameter name for BigQuery

---

### 6. Centralized Authentication (NEW)

**CRITICAL CHANGE**: Authentication ID now comes from centralized config

```yaml
# In load config:
td_authentication_id: ${td_authentication_ids.google_bq.default}

# NOT:
td_authentication_id: 360071  # ← Old hardcoded approach
```

**Why**: Centralized auth makes it easier to update IDs across all sources

---

## Adding New BigQuery Data Sources

### Step 1: Add to Datasources Config

Add new entry to `config/google_bq_datasources.yml`:

**For Historical**:
```yaml
hist_datasources:
  - name: [bigquery_table_name_historical]
    table: gbq_[table_name]_hist
    dataset: [dataset_name]
    config_file: google_bq_[table_name]_historical.yml
    workflow_name: google_bigquery_ingest_hist.dig
    mode: append
    inc_field: [incremental_field_name]  # or "" for full load
    default_start_time: "YYYY-MM-DD HH:mm:ss.SSS"
```

**For Incremental**:
```yaml
inc_datasources:
  - name: [bigquery_table_name]
    table: gbq_[table_name]
    dataset: [dataset_name]
    config_file: google_bq_[table_name].yml
    workflow_name: google_bigquery_ingest_inc.dig
    mode: append
    inc_field: [incremental_field_name]  # or "" for full load
    default_start_time: "YYYY-MM-DD HH:mm:ss.SSS"
```

### Step 2: Create Load Config Files

**For Incremental (Query)**:

Create `config/google_bq_[table_name].yml`:
```yaml
in:
  type: bigquery_v2
  td_authentication_id: ${td_authentication_ids.google_bq.default}
  project_id: prj-dgtl-p-data-ext-prvdr-3157
  dataset: ${datasource.dataset}
  import_type: query
  query: SELECT * FROM ${datasource.dataset}.${datasource.name} WHERE ${datasource.inc_field} > '${td.last_results.start_time}'
  export_to_gcs: false
  max_connection_retry: 10

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

**For Historical (Table)**:

Create `config/google_bq_[table_name]_historical.yml`:
```yaml
in:
  type: bigquery_v2
  td_authentication_id: ${td_authentication_ids.google_bq.default}
  project_id: prj-dgtl-p-data-ext-prvdr-3157
  import_type: table
  dataset: ${datasource.dataset}
  table: ${datasource.name}

  export_to_gcs: false
  max_connection_retry: 10

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

### Step 3: Test and Deploy

```bash
# Test historical workflow
td wf run google_bigquery_ingest_hist.dig

# Test incremental workflow
td wf run google_bigquery_ingest_inc.dig

# Deploy to production
td wf push ingestion
```

---

## BigQuery Authentication

### Authentication ID (Centralized)

**CRITICAL CHANGE**: Now configured in `config/database.yml`:

```yaml
td_authentication_ids:
  google_bq:
    default: 360071
```

Referenced in load configs as:
```yaml
td_authentication_id: ${td_authentication_ids.google_bq.default}
```

### Project ID

```yaml
project_id: prj-dgtl-p-data-ext-prvdr-3157
```

Google Cloud Project ID where BigQuery datasets reside.

---

## Common BigQuery Issues

### Issue: "Dataset not found"
**Cause**: Wrong dataset name or missing permissions

**Solution**:
1. Verify dataset name in BigQuery console
2. Check service account has `BigQuery Data Viewer` role
3. Ensure project_id is correct

### Issue: "No data imported" (incremental)
**Cause**: start_time format mismatch

**Solution**:
1. Verify default_start_time uses SQL Server format (`YYYY-MM-DD HH:mm:ss.SSS`)
2. Check inc_field actually exists in BigQuery table
3. Test with full table import first

### Issue: "Query timeout"
**Cause**: Complex query or large dataset

**Solution**:
1. Use `export_to_gcs: false` to avoid GCS export overhead
2. Increase `max_connection_retry`
3. Add more specific WHERE clauses to query
4. Consider partitioning by date range

### Issue: "Table already exists" error
**Cause**: Mode is set to replace but table creation is attempted

**Solution**:
1. Use `mode: append` for incremental
2. Ensure `CREATE TABLE IF NOT EXISTS` is used in workflow
3. Check table naming conventions

### Issue: Monthly batch not skipping
**Cause**: Source name doesn't match in logging

**Solution**:
1. Verify source_name uses pattern: `'google_bigquery_${date_range.month_name}'`
2. Check ingestion_log table for exact source_name values
3. Ensure month_name format matches in hist_date_ranges.yml

---

## Performance Optimization

### 1. Use Different Parallel Settings by Mode

```yaml
# Historical: Unlimited for fast backfill
_parallel: true

# Incremental: Limited to avoid API pressure
_parallel:
  limit: 3
```

### 2. Optimize Query Filters

```sql
WHERE ${datasource.inc_field} > '${td.last_results.start_time}'
  AND partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
```

### 3. Disable GCS Export

```yaml
export_to_gcs: false
```

### 4. Retry Configuration

```yaml
max_connection_retry: 10
```

### 5. Monthly Batching for Historical

Allows parallel processing of multiple months while avoiding reprocessing

---

## Workflow Execution Strategy

### Initial Setup (Historical Backfill)
1. Run `google_bigquery_ingest_hist.dig` to load all historical data
2. Uses monthly batches with skip logic
3. Can be resumed if interrupted (skips completed months)
4. Uses unlimited parallel for fast processing

### Ongoing Updates (Incremental)
1. Run `google_bigquery_ingest_inc.dig` daily/hourly
2. Uses `inc_field` to get only new/updated records
3. Faster and lighter than historical
4. Uses limited parallel (3 concurrent jobs)

---

## Critical Reminders

1. **ALWAYS use SQL Server timestamp format** (`yyyy-MM-dd HH:mm:ss.SSS`)
2. **ALWAYS use `inc_field`** (NOT `incremental_field`) for BigQuery
3. **ALWAYS use `${td_authentication_ids.google_bq.default}`** for auth
4. **ALWAYS use `log_ingestion_success.sql`** for success logging (NOT success_2)
5. **USE unlimited parallel for historical** (`_parallel: true`)
6. **USE limited parallel for incremental** (`_parallel: {limit: 3}`)
7. **USE `import_type: query`** for incremental, `import_type: table` for historical
8. **EMBED inc_field in query string** for incremental filtering
9. **SET `export_to_gcs: false`** for better performance
10. **CHECK authentication** has proper BigQuery permissions
11. **TEST with full load first** before enabling incremental
12. **USE monthly batches** for historical to enable resuming
13. **VERIFY batch skip logic** is working via ingestion_log

---

## Data Source Reference

| Table | Historical Field | Incremental Field | Import Type (Hist) | Import Type (Inc) |
|-------|-----------------|-------------------|-------------------|-------------------|
| shopify_order | incremental_date | incremental_date | table | query |
| shopify_transaction | incremental_date | incremental_date | table | query |
| shopify_order_adjustments | incremental_date | incremental_date | table | query |
| shopify_discount_allocation | incremental_date | incremental_date | table | query |
| shopify_order_line | incremental_date | incremental_date | table | query |
| gold_user_profile | N/A | N/A | table | table |

---

## Reference

**Related Documentation**:
- Timestamp formats: `docs/patterns/timestamp-formats.md`
- Logging patterns: `docs/patterns/logging-patterns.md`
- Workflow patterns: `docs/patterns/workflow-patterns.md`
- Monthly batch processing: `docs/sources/onetrust.md` (similar pattern)

**Connector Documentation**:
- [Treasure Data BigQuery v2 Connector](https://docs.treasuredata.com/display/public/PD/BigQuery+Import+Integration)
