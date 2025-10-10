# Incremental Field Patterns

## Overview
Incremental data processing requires careful handling of field names, as they can differ between the Treasure Data table and the connector API. This document provides the EXACT patterns for handling incremental fields.

## Critical Concept: Dual Field Names

**CRITICAL**: Some connectors use DIFFERENT field names in:
1. **Table Field** (`incremental_field`): Field name in the Treasure Data table
2. **Connector Field** (`incremental_field_connector`): Field name the connector API expects

**Example (Klaviyo Campaigns)**:
- Table stores: `created_at` (what you query in SQL)
- API expects: `created` (what you pass to the connector)

---

## incremental_field vs. incremental_field_connector

### Where Each Is Used

| Location | Use `incremental_field` | Use `incremental_field_connector` |
|----------|-------------------------|----------------------------------|
| SQL Queries (workflow .dig files) | ✅ YES | ❌ NO |
| Table creation (CREATE TABLE) | ✅ YES | ❌ NO |
| Load config (.yml files) | ❌ NO | ✅ YES |
| Datasources config | ✅ BOTH | ✅ BOTH |

---

## Exact Pattern: Datasources Configuration

**File**: `config/[source]_datasources.yml`

```yaml
hist_datasources:
  - name: [api_object_name]
    table_name: [source]_[object]_hist
    config_file: [source]_[object]_load.yml
    incremental_field: [field_in_table]           # Used in SQL queries
    incremental_field_connector: [field_in_api]   # Used in load config
    default_start_time: "YYYY-MM-DDTHH:MM:SS.000000"
    workflow_name: [source]_ingest.dig
    mode: append

inc_datasources:
  - name: [api_object_name]
    table_name: [source]_[object]
    config_file: [source]_[object]_load.yml
    incremental_field: [field_in_table]           # Used in SQL queries
    incremental_field_connector: [field_in_api]   # Used in load config
    default_start_time: "YYYY-MM-DDTHH:MM:SS.000000"
    workflow_name: [source]_ingest.dig
    mode: append
```

---

## Exact Pattern: Workflow (Using incremental_field)

**In Workflow `.dig` Files**: ALWAYS use `incremental_field` for:
- CREATE TABLE statements
- SELECT queries to get max value
- WHERE clauses

```yaml
+create_incremental_table:
  td>:
  query: |
    CREATE TABLE IF NOT EXISTS ${databases.src}.${datasource.table_name} (
      ${datasource.incremental_field} VARCHAR,  # ← Table field
      time BIGINT
    )
  database: ${databases.src}

+get_last_incremental_time:
  td>:
  query: |
    SELECT
      COALESCE(
        (SELECT [CONVERSION_FUNCTION](max(${datasource.incremental_field})) as start_time  # ← Table field
          FROM ${databases.src}.${datasource.table_name}
          WHERE ${datasource.incremental_field} IS NOT NULL),  # ← Table field
        '${datasource.default_start_time}'
      ) as start_time,
      [END_TIME_FUNCTION] as end_time
  database: ${databases.src}
  store_last_results: true
```

---

## Exact Pattern: Load Config (Using incremental_field_connector)

**In Load Config `.yml` Files**: ALWAYS use `incremental_field_connector`

```yaml
in:
  type: [connector_type]
  td_authentication_id: [auth_id]
  data_source: ${datasource.name}
  incremental: true
  start_time: ${td.last_results.start_time}
  end_time: ${td.last_results.end_time}
  incremental_field: ${datasource.incremental_field_connector}  # ← Connector field!

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

**CRITICAL**: The `incremental_field:` parameter in the load config MUST use `incremental_field_connector`

---

## Field Name Mappings by Source

### Klaviyo Field Mappings

| Data Source | Table Field (`incremental_field`) | API Field (`incremental_field_connector`) | Notes |
|------------|-----------------------------------|------------------------------------------|-------|
| **Events** (hist) | `datetime` | `datetime` | Same for both |
| **Events** (inc) | `datetime` | `datetime` | Same for both |
| **Templates** (hist) | `created` | `created` | Same for both |
| **Templates** (inc) | `updated` | `updated` | Same for both |
| **Campaigns** (hist) | `created_at` | `created` | **DIFFERENT** |
| **Campaigns** (inc) | `updated_at` | `updated` | **DIFFERENT** |
| **Lists** (hist) | `created` | `created` | Same for both |
| **Lists** (inc) | `updated` | `updated` | Same for both |
| **Profiles** (hist) | `created` | `created` | Same for both |
| **Profiles** (inc) | `updated` | `updated` | Same for both |
| **Metrics** | N/A | N/A | Non-incremental |

### OneTrust Field Mappings

| Data Source | Table Field | API Field | Notes |
|------------|-------------|-----------|-------|
| Data Subject Profile | `updated_date` | `updated_date` | Same for both |
| Collection Point | `updated_date` | `updated_date` | Same for both |

### Shopify v2 Field Mappings

| Data Source | Historical Field | Incremental Field | Notes |
|------------|-----------------|-------------------|-------|
| Products | `created_at` | `updated_at` | Different for hist vs. inc |
| Product Variants | `created_at` | `updated_at` | Different for hist vs. inc |

### Google BigQuery Field Mappings

| Data Source | Incremental Field | Notes |
|------------|-------------------|-------|
| All tables | `inc_field` | Uses different parameter name |

**Note**: BigQuery uses `inc_field` instead of `incremental_field` in datasources config

---

## Empty Incremental Fields (Non-Incremental Sources)

For sources that don't support incremental loading:

### Datasources Configuration
```yaml
inc_datasources:
  - name: metrics
    table_name: klaviyo_metric_data
    config_file: klaviyo_metric_data_load.yml
    incremental_field:                    # Empty string or omit
    incremental_field_connector:          # Empty string or omit
    default_start_time: "2025-09-01T00:00:00.000000"
    workflow_name: klaviyo_ingest.dig
    mode: append
```

### Workflow Pattern
```yaml
+setup_table_and_time:
  if>: ${datasource.incremental_field != "" && datasource.incremental_field != null}
  _do:
    [Incremental setup]
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
          '' as start_time,  # Empty start time
          [END_TIME_FUNCTION] as end_time
      database: ${databases.src}
      store_last_results: true
```

### Load Config Pattern
```yaml
in:
  type: klaviyo
  data_source: ${datasource.name}
  td_authentication_id: 360030
  # NO incremental parameters

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

## Special Cases

### Case 1: Klaviyo Events (No incremental_field in Config)

**Datasources Config**:
```yaml
- name: events
  table_name: klaviyo_events
  config_file: klaviyo_events_load.yml
  incremental_field: datetime              # For SQL queries
  incremental_field_connector: datetime    # For workflow logic
  default_start_time: "2025-09-01T00:00:00.000000"
  workflow_name: klaviyo_ingest.dig
  mode: append
```

**Load Config** (`klaviyo_events_load.yml`):
```yaml
in:
  type: klaviyo
  data_source: ${datasource.name}
  td_authentication_id: 360030
  skip_related_objects: true
  incremental: true
  start_time: ${td.last_results.start_time}
  end_time: ${td.last_results.end_time}
  # NO incremental_field parameter! Events use start_time/end_time only

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

**CRITICAL**: Events do NOT use `incremental_field:` in the load config, even though they have incremental fields in the datasources config

---

### Case 2: Google BigQuery (Different Parameter Name)

**Datasources Config**:
```yaml
datasources:
  - name: shopify_order
    table: gbq_shopify_order
    dataset: gold_treasure_data
    config_file: google_bq_shopify_order.yml
    workflow_name: google_bigquery_ingest.dig
    mode: append
    inc_field: incremental_date          # Note: inc_field not incremental_field
    default_start_time: "2025-08-31 23:59:59.000"
```

**Workflow**:
```yaml
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

**Load Config**:
```yaml
in:
  type: bigquery_v2
  td_authentication_id: 360071
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

**Note**: BigQuery uses `inc_field` everywhere and embeds it in the SQL query string

---

## Incremental Field Checking Pattern

**ALWAYS check if incremental_field exists before using it**:

```yaml
+setup_table_and_time:
  if>: ${datasource.incremental_field != "" && datasource.incremental_field != null}
  _do:
    [Incremental setup with field]
  _else_do:
    [Non-incremental setup]
```

This prevents errors when processing non-incremental sources in the same workflow.

---

## Historical vs. Incremental Field Differences

Some sources use DIFFERENT fields for historical vs. incremental:

### Shopify v2 Example

**Historical Datasources** (created_at):
```yaml
hist_datasources:
  - name: products
    table_name: shopify_products_hist
    config_file: shopify_v2_products_load.yml
    incremental_field: created_at         # Historical uses created
    default_start_time: "2023-01-01T00:00:00.000Z"
    workflow_name: shopify_v2_ingest_hist.dig
    mode: append
```

**Incremental Datasources** (updated_at):
```yaml
inc_datasources:
  - name: products
    table_name: shopify_products
    config_file: shopify_v2_products_load.yml
    incremental_field: updated_at         # Incremental uses updated
    default_start_time: "2025-01-01T00:00:00.000Z"
    workflow_name: shopify_v2_ingest_inc.dig
    mode: append
```

**Load Config** (same file, uses variable):
```yaml
in:
  type: shopify_v2
  td_authentication_id: 360360
  target: products
  incremental: true
  from_date: ${td.last_results.start_time}
  to_date: ${td.last_results.end_time}
  incremental_field: ${datasource.incremental_field}  # Uses whichever is configured
  include_image: true
```

---

## Troubleshooting Incremental Field Issues

### Error: "Field not found in table"
**Cause**: Using `incremental_field_connector` in SQL query instead of `incremental_field`

**Solution**: Use `${datasource.incremental_field}` in all SQL queries

### Error: "Unknown incremental field" from connector
**Cause**: Using `incremental_field` in load config instead of `incremental_field_connector`

**Solution**: Use `${datasource.incremental_field_connector}` in load config YAML

### Error: "No data ingested" (incremental workflow)
**Cause**: Mismatched field names or wrong field in datasources config

**Solution**:
1. Verify connector API documentation for correct field name
2. Check actual table schema for stored field name
3. Ensure both fields are defined in datasources config

### Error: "NULL pointer exception on incremental_field"
**Cause**: Non-incremental source but workflow expects incremental field

**Solution**: Add conditional check:
```yaml
if>: ${datasource.incremental_field != "" && datasource.incremental_field != null}
```

---

## Quick Reference Checklist

When setting up incremental fields:

- [ ] Checked connector API docs for correct field name
- [ ] Defined both `incremental_field` and `incremental_field_connector` (or confirmed they're the same)
- [ ] Used `incremental_field` in all SQL queries (CREATE TABLE, SELECT, WHERE)
- [ ] Used `incremental_field_connector` in load config YAML
- [ ] Added null check for non-incremental sources
- [ ] Verified historical vs. incremental use different fields (if applicable)
- [ ] Tested with small date range first
- [ ] Confirmed data actually has the incremental field populated

---

## Critical Reminders

1. **ALWAYS define both fields** in datasources config (even if identical)
2. **NEVER use `incremental_field_connector` in SQL** queries
3. **NEVER use `incremental_field` in load config** (except BigQuery which uses `inc_field`)
4. **ALWAYS check for null/empty** before using incremental_field
5. **VERIFY API field name** from connector documentation
6. **TEST with existing data** to ensure field names match
7. **REMEMBER**: Klaviyo Events don't use `incremental_field:` in load config
8. **REMEMBER**: Campaigns have `_at` suffix in table but not in API
