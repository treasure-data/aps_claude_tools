# Klaviyo Ingestion - Exact Templates

## Overview
Klaviyo connector handles marketing automation data including profiles, events, campaigns, lists, email templates, and metrics. This source has unique patterns including dual incremental fields and special handling for events.

## Source Characteristics

| Characteristic | Value |
|---------------|-------|
| **Connector Type** | `klaviyo` |
| **Parallel Processing** | Limited (`_parallel: {limit: 3}`) |
| **Timestamp Format** | `.000000` (6 decimals, NO Z) |
| **Incremental Fields** | Dual (table ≠ API for some sources) |
| **Success Logging** | `log_ingestion_success.sql` (with time tracking) |
| **Data Sources** | Events, Templates, Campaigns, Lists, Profiles, Metrics |

---

## Workflow Files

### Incremental Workflow

**File**: `klaviyo_ingest_inc.dig`

**EXACT TEMPLATE**:
```yaml
timezone: UTC

_export:
  !include : config/database.yml
  !include : config/klaviyo_datasources.yml
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
                (SELECT replace(TD_TIME_STRING(TD_TIME_PARSE(max(${datasource.incremental_field})), 's!'), ' ', 'T') || '.000000' as start_time
                  FROM ${databases.src}.${datasource.table_name}
                  WHERE ${datasource.incremental_field} IS NOT NULL),
                '${datasource.default_start_time}'
              ) as start_time,
              SUBSTR(CAST(to_iso8601(current_timestamp) AS VARCHAR),1,19)||'.000000' as end_time
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
              '' as start_time,
              SUBSTR(CAST(to_iso8601(current_timestamp) AS VARCHAR),1,19)||'.000000' as end_time
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
    source_name: 'klaviyo'
    workflow_name: 'klaviyo_ingest'
    error_message: 'klaviyo ingestion failed. check klaviyo_ingest'
```

### Historical Workflow

**File**: `klaviyo_ingest_hist.dig`

Structure identical to incremental, but uses `${hist_datasources}` instead of `${inc_datasources}`.

---

## Datasources Configuration

**File**: `config/klaviyo_datasources.yml`

**EXACT TEMPLATE**:
```yaml
hist_datasources:
  - name: events
    table_name: klaviyo_events_hist
    config_file: klaviyo_events_load.yml
    incremental_field: datetime
    incremental_field_connector: datetime
    default_start_time: "2023-09-01T00:00:00.000000"
    workflow_name: klaviyo_ingest.dig
    mode: append

  - name: templates
    table_name: klaviyo_email_templates_hist
    config_file: klaviyo_email_templates_load.yml
    incremental_field: created
    incremental_field_connector: created
    default_start_time: "2023-09-01T00:00:00.000000"
    workflow_name: klaviyo_ingest.dig
    mode: append

  - name: campaigns
    table_name: klaviyo_campaigns_hist
    config_file: klaviyo_campaigns_load.yml
    incremental_field: created_at
    incremental_field_connector: created
    default_start_time: "2023-09-01T00:00:00.000000"
    workflow_name: klaviyo_ingest.dig
    mode: append

  - name: lists
    table_name: klaviyo_lists_hist
    config_file: klaviyo_lists_load.yml
    incremental_field: created
    incremental_field_connector: created
    default_start_time: "2023-09-01T00:00:00.000000"
    workflow_name: klaviyo_ingest.dig
    mode: append

  - name: profiles
    table_name: klaviyo_profiles_hist
    config_file: klaviyo_profiles_load.yml
    incremental_field: created
    incremental_field_connector: created
    default_start_time: "2023-09-01T00:00:00.000000"
    workflow_name: klaviyo_ingest.dig
    mode: append

inc_datasources:
  - name: events
    table_name: klaviyo_events
    config_file: klaviyo_events_load.yml
    incremental_field: datetime
    incremental_field_connector: datetime
    default_start_time: "2025-09-01T00:00:00.000000"
    workflow_name: klaviyo_ingest.dig
    mode: append

  - name: templates
    table_name: klaviyo_email_templates
    config_file: klaviyo_email_templates_load.yml
    incremental_field: updated
    incremental_field_connector: updated
    default_start_time: "2025-09-01T00:00:00.000000"
    workflow_name: klaviyo_ingest.dig
    mode: append

  - name: campaigns
    table_name: klaviyo_campaigns
    config_file: klaviyo_campaigns_load.yml
    incremental_field: updated_at
    incremental_field_connector: updated
    default_start_time: "2025-09-01T00:00:00.000000"
    workflow_name: klaviyo_ingest.dig
    mode: append

  - name: lists
    table_name: klaviyo_lists
    config_file: klaviyo_lists_load.yml
    incremental_field: updated
    incremental_field_connector: updated
    default_start_time: "2025-09-01T00:00:00.000000"
    workflow_name: klaviyo_ingest.dig
    mode: append

  - name: profiles
    table_name: klaviyo_profiles
    config_file: klaviyo_profiles_load.yml
    incremental_field: updated
    incremental_field_connector: updated
    default_start_time: "2025-09-01T00:00:00.000000"
    workflow_name: klaviyo_ingest.dig
    mode: append

  - name: metrics
    table_name: klaviyo_metric_data
    config_file: klaviyo_metric_data_load.yml
    incremental_field:
    incremental_field_connector:
    default_start_time: "2025-09-01T00:00:00.000000"
    workflow_name: klaviyo_ingest.dig
    mode: append
```

---

## Critical Klaviyo Patterns

### 1. Dual Incremental Field Names

**CRITICAL**: Campaigns use DIFFERENT field names in table vs. API

| Data Source | Table Field | API Field | Match? |
|------------|-------------|-----------|--------|
| Events | `datetime` | `datetime` | ✅ Same |
| Templates (hist) | `created` | `created` | ✅ Same |
| Templates (inc) | `updated` | `updated` | ✅ Same |
| **Campaigns (hist)** | `created_at` | `created` | ❌ **DIFFERENT** |
| **Campaigns (inc)** | `updated_at` | `updated` | ❌ **DIFFERENT** |
| Lists (hist) | `created` | `created` | ✅ Same |
| Lists (inc) | `updated` | `updated` | ✅ Same |
| Profiles (hist) | `created` | `created` | ✅ Same |
| Profiles (inc) | `updated` | `updated` | ✅ Same |
| Metrics | N/A | N/A | Non-incremental |

**Pattern**: Campaigns have `_at` suffix in table but NOT in API

---

## Load Configuration Files

### Type 1: Standard Incremental (Profiles, Lists, Templates, Campaigns)

**File**: `config/klaviyo_profiles_load.yml`

**EXACT TEMPLATE**:
```yaml
in:
  type: klaviyo
  data_source: ${datasource.name}
  td_authentication_id: 360030
  incremental: true
  start_time: ${td.last_results.start_time}
  end_time: ${td.last_results.end_time}
  incremental_field: ${datasource.incremental_field_connector}

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

**CRITICAL**: Uses `incremental_field_connector` NOT `incremental_field`

---

### Type 2: Events (Special Handling)

**File**: `config/klaviyo_events_load.yml`

**EXACT TEMPLATE**:
```yaml
in:
  type: klaviyo
  data_source: ${datasource.name}
  td_authentication_id: 360030
  skip_related_objects: true
  incremental: true
  start_time: ${td.last_results.start_time}
  end_time: ${td.last_results.end_time}

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

**CRITICAL Differences**:
1. Has `skip_related_objects: true`
2. **NO `incremental_field` parameter** (uses start_time/end_time only)

---

### Type 3: Metrics (Non-Incremental)

**File**: `config/klaviyo_metric_data_load.yml`

**EXACT TEMPLATE**:
```yaml
in:
  type: klaviyo
  data_source: ${datasource.name}
  td_authentication_id: 360030

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

**CRITICAL**: No incremental parameters at all - full load every time

---

## Klaviyo Timestamp Functions

### Start Time (Get Max from Table)
```sql
replace(TD_TIME_STRING(TD_TIME_PARSE(max(${datasource.incremental_field})), 's!'), ' ', 'T') || '.000000'
```

**Output**: `2023-09-01T00:00:00.000000`

### End Time (Current Timestamp)
```sql
SUBSTR(CAST(to_iso8601(current_timestamp) AS VARCHAR),1,19)||'.000000'
```

**Output**: `2025-10-10T13:45:30.000000`

**Format**: 6 decimals, NO Z suffix

---

## Authentication

**Authentication ID**: `360030`

Configured in Treasure Data console under Authentications for Klaviyo API.

**Required Secrets** (in `credentials_ingestion.json`):
```json
{
  "klaviyo_api_token": "your_private_api_key",
  "klaviyo_revision_time": "2023-02-22"
}
```

**Note**: Current implementation uses `td_authentication_id` instead of secrets directly.

---

## Adding New Klaviyo Data Sources

### Step 1: Verify API Field Name

Check Klaviyo API documentation for the data source's incremental field name.

**Examples**:
- Events use `datetime`
- Profiles use `created` (hist) or `updated` (inc)
- Campaigns use `created` (hist) or `updated` (inc) in API, but `created_at`/`updated_at` in table

### Step 2: Add to Datasources Config

Add to `config/klaviyo_datasources.yml`:

```yaml
inc_datasources:
  - name: [klaviyo_object_name]
    table_name: klaviyo_[object_name]
    config_file: klaviyo_[object_name]_load.yml
    incremental_field: [field_in_table]
    incremental_field_connector: [field_in_api]
    default_start_time: "2025-09-01T00:00:00.000000"
    workflow_name: klaviyo_ingest.dig
    mode: append
```

### Step 3: Create Load Config

Create `config/klaviyo_[object_name]_load.yml`:

**If incremental field is supported**:
```yaml
in:
  type: klaviyo
  data_source: ${datasource.name}
  td_authentication_id: 360030
  incremental: true
  start_time: ${td.last_results.start_time}
  end_time: ${td.last_results.end_time}
  incremental_field: ${datasource.incremental_field_connector}

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

**If events-like (no incremental field parameter)**:
```yaml
in:
  type: klaviyo
  data_source: ${datasource.name}
  td_authentication_id: 360030
  skip_related_objects: true
  incremental: true
  start_time: ${td.last_results.start_time}
  end_time: ${td.last_results.end_time}

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
td wf run klaviyo_ingest_inc.dig
```

---

## Common Klaviyo Issues

### Issue: "Unknown incremental field"
**Cause**: Using table field name (`created_at`) instead of API field name (`created`)

**Solution**: Verify `incremental_field_connector` matches API documentation exactly

### Issue: Events not ingesting
**Cause**: Missing `skip_related_objects: true` or incorrect incremental_field parameter

**Solution**:
1. Add `skip_related_objects: true` to events config
2. Remove `incremental_field:` parameter from events config (use start_time/end_time only)

### Issue: "Invalid timestamp format"
**Cause**: Wrong decimal count or missing 'T' separator

**Solution**:
1. Verify using `.000000` (6 decimals)
2. Check 'T' separator between date and time
3. Ensure NO 'Z' suffix

### Issue: No data for metrics
**Cause**: Trying to use incremental for non-incremental source

**Solution**: Remove all incremental parameters from metrics config - it's a full load

### Issue: Campaigns field mismatch
**Cause**: Using `created` or `updated` in table queries

**Solution**: Use `created_at` or `updated_at` in table, `created` or `updated` in API

---

## Performance Optimization

### 1. Use Parallel Limit
```yaml
_parallel:
  limit: 3  # Respects Klaviyo API rate limits
```

### 2. Skip Related Objects for Events
```yaml
skip_related_objects: true  # Reduces payload size
```

### 3. Incremental Time Ranges
Use appropriate `default_start_time` to avoid loading excessive historical data.

---

## Critical Reminders

1. **ALWAYS use `.000000` timestamp format** (6 decimals, NO Z)
2. **ALWAYS use `incremental_field_connector`** in load configs
3. **ALWAYS use `incremental_field`** in SQL queries
4. **REMEMBER**: Campaigns have `_at` suffix in table but NOT in API
5. **REMEMBER**: Events use NO `incremental_field` parameter in config
6. **REMEMBER**: Events require `skip_related_objects: true`
7. **REMEMBER**: Metrics are non-incremental (full load)
8. **USE `_parallel: {limit: 3}`** to respect API rate limits
9. **VERIFY** field names against Klaviyo API documentation
10. **TEST** with small date ranges first

---

## Data Source Reference

| Data Source | Incremental? | Special Config | Table Field (inc) | API Field (inc) |
|------------|--------------|----------------|-------------------|-----------------|
| Events | Yes | skip_related_objects, NO incremental_field param | datetime | datetime |
| Templates | Yes | Standard | updated | updated |
| Campaigns | Yes | Standard | updated_at | updated |
| Lists | Yes | Standard | updated | updated |
| Profiles | Yes | Standard | updated | updated |
| Metrics | No | No incremental params | N/A | N/A |

---

## Reference

**Related Documentation**:
- Timestamp formats: `docs/patterns/timestamp-formats.md`
- Incremental patterns: `docs/patterns/incremental-patterns.md`
- Logging patterns: `docs/patterns/logging-patterns.md`
- Workflow patterns: `docs/patterns/workflow-patterns.md`

**Connector Documentation**:
- [Treasure Data Klaviyo Connector](https://docs.treasuredata.com/display/public/PD/Klaviyo+Import+Integration)
