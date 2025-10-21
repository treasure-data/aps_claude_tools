# Shopify v1 Data Ingestion - Exact Templates

**IMPORTANT: This document contains EXACT, production-ready templates. Copy character-for-character, line-by-line. Do NOT improvise or simplify.**

**⚠️ LEGACY WARNING: Shopify v1 connector is legacy. For new implementations, use Shopify v2 (see `shopify-v2.md`).**

---

## Overview

Shopify v1 data ingestion uses the **Shopify v1 connector** (legacy) to load customer and inventory data. This implementation follows the **separate workflows pattern** with incremental and historical workflows.

### Key Characteristics

| Feature | Value |
|---------|-------|
| **Connector Type** | Shopify v1 (legacy API) |
| **Incremental** | Yes - timestamp-based |
| **Historical** | Separate workflow with monthly batches |
| **Parallel (inc)** | Yes (limit: 3) |
| **Parallel (hist)** | Yes (limit: 3 per datasource, unlimited datasources) |
| **Timestamp Format** | ISO 8601 (`.000Z`) |
| **Incremental Field (hist)** | `created_at` |
| **Incremental Field (inc)** | `updated_at` |
| **Tables** | customers, inventory_items |

---

## Critical Implementation Details

### ⚠️ CRITICAL: Shopify v1 vs Shopify v2

| Feature | Shopify v1 | Shopify v2 |
|---------|-----------|------------|
| **Status** | Legacy | Current |
| **Connector** | `type: shopify` | `type: shopify_v2` |
| **Auth ID** | `shopify_v1.mccormick_flavors` | `shopify_v2.default` |
| **Custom Column** | `shop_flavor_flag` | None |
| **Use Case** | Specific shop only | General purpose |

### ⚠️ CRITICAL: Separate Incremental Fields

- **Historical**: Uses `created_at` to load all historical records
- **Incremental**: Uses `updated_at` to load only updates
- **NEVER mix** these fields in the same workflow

---

## File Structure

```
ingestion/
├── shopify_v1_ingest_inc.dig              # Incremental workflow
├── shopify_v1_ingest_hist.dig             # Historical workflow
├── config/
│   ├── database.yml                       # Shared database config
│   ├── hist_date_ranges.yml               # Historical date ranges
│   ├── shopify_v1_datasources.yml         # Data source configuration
│   ├── shopify_v1_customers_load.yml      # Customers load config
│   └── shopify_v1_inventory_items_load.yml # Inventory items load config
└── sql/
    ├── log_ingestion_start.sql            # Start logging
    ├── log_ingestion_success.sql          # Success logging
    └── log_ingestion_error.sql            # Error logging
```

---

## Complete File Templates

### 1. Incremental Workflow: `shopify_v1_ingest_inc.dig`

**EXACT TEMPLATE - Copy character-for-character:**

```yaml
timezone: UTC

_export:
  !include : config/database.yml
  !include : config/shopify_v1_datasources.yml

+load_incremental_data:
  _parallel:
    limit: 3
  for_each>:
    datasource: ${inc_datasources}
  _do:
    +log_ingestion_start:
      td>: sql/log_ingestion_start.sql
      database: ${databases.src}
      source_name: 'shopify'
      workflow_name: 'shopify_v1_ingest'
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
                (SELECT replace(TD_TIME_STRING(TD_TIME_PARSE(max(${datasource.incremental_field})), 's!'), ' ', 'T') || '.000Z' as start_time
                  FROM ${databases.src}.${datasource.table_name}
                  WHERE ${datasource.incremental_field} IS NOT NULL),
                '${datasource.default_start_time}'
              ) as start_time,
              to_iso8601(current_timestamp) as end_time
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
              to_iso8601(current_timestamp) as end_time
          database: ${databases.src}
          store_last_results: true

    +load_incremental:
      td_load>: config/${datasource.config_file}
      database: ${databases.src}
      table: ${datasource.table_name}

    +log_ingestion_success:
      td>: sql/log_ingestion_success.sql
      database: ${databases.src}
      source_name: 'shopify'
      workflow_name: 'shopify_v1_ingest'
      table_name: ${datasource.table_name}

_error:
  +log_ingestion_error:
    td>: sql/log_ingestion_error.sql
    database: ${databases.src}
    source_name: 'shopify_v1'
    workflow_name: 'shopify_v1_ingest'
    error_message: 'shopify v1 ingestion failed. check shopify_v1_ingest'
```

---

### 2. Historical Workflow: `shopify_v1_ingest_hist.dig`

**EXACT TEMPLATE - Copy character-for-character:**

```yaml
timezone: UTC

_export:
  !include : config/database.yml
  !include : config/shopify_v1_datasources.yml
  !include : config/hist_date_ranges.yml


+load_historical_data:
  _parallel: true
  for_each>:
    datasource: ${hist_datasources}
  _do:
    +log_ingestion_start:
      td>: sql/log_ingestion_start.sql
      database: ${databases.src}
      source_name: 'shopify_v1'
      workflow_name: 'shopify_v1_ingest'
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
                    source_name = 'shopify_v1_${date_range.month_name}'
                    AND workflow_name = 'shopify_v1_ingest'
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
                  replace('${date_range.start_time}', '.000000', '.000Z') as start_time,
                  replace('${date_range.end_time}', '.000000', '.000Z') as end_time
              database: ${databases.src}
              store_last_results: true

            +log_batch_start:
              td>: sql/log_ingestion_start.sql
              database: ${databases.src}
              source_name: 'shopify_v1_${date_range.month_name}'
              workflow_name: 'shopify_v1_ingest'
              table_name: ${datasource.table_name}

            +load_batch:
              td_load>: config/${datasource.config_file}
              database: ${databases.src}
              table: ${datasource.table_name}

            +log_batch_success:
              td>: sql/log_ingestion_success.sql
              database: ${databases.src}
              source_name: 'shopify_v1_${date_range.month_name}'
              workflow_name: 'shopify_v1_ingest'
              table_name: ${datasource.table_name}
          _else_do:
            +skip_batch:
              echo>: "Skipping ${date_range.month_name} for ${datasource.table_name} - already processed successfully"

    +log_ingestion_success:
      td>: sql/log_ingestion_success.sql
      database: ${databases.src}
      source_name: 'shopify_v1'
      workflow_name: 'shopify_v1_ingest'
      table_name: ${datasource.table_name}

_error:
  +log_ingestion_error:
    td>: sql/log_ingestion_error.sql
    database: ${databases.src}
    source_name: 'shopify_v1'
    workflow_name: 'shopify_v1_ingest'
    error_message: 'shopify v1 ingestion failed. check shopify_v1_ingest'
```

---

### 3. Datasource Config: `shopify_v1_datasources.yml`

**EXACT TEMPLATE - Copy character-for-character:**

```yaml
hist_datasources:
  - name: customers
    table_name: shopify_customers_hist
    config_file: shopify_v1_customers_load.yml
    incremental_field: created_at
    default_start_time: "2023-09-01T00:00:00.000Z"
    workflow_name: shopify_v1_ingest_hist.dig
    mode: append

  # - name: inventory_items
  #   table_name: shopify_inventory_items_hist
  #   config_file: shopify_v1_inventory_items_load.yml
  #   incremental_field:
  #   default_start_time: "2023-09-01T00:00:00.000Z"
  #   workflow_name: shopify_v1_ingest_hist.dig
  #   mode: append


inc_datasources:
  - name: customers
    table_name: shopify_customers
    config_file: shopify_v1_customers_load.yml
    incremental_field: updated_at
    default_start_time: "2025-09-01T00:00:00.000Z"
    workflow_name: shopify_v1_ingest_inc.dig
    mode: append

  # - name: inventory_items
  #   table_name: shopify_inventory_items
  #   config_file: shopify_v1_inventory_items_load.yml
  #   incremental_field: created_at
  #   default_start_time: "2025-09-01T00:00:00.000Z"
  #   workflow_name: shopify_v1_ingest_inc.dig
  #   mode: append
```

**Template Placeholders:**
- `{object}`: Shopify object name (customers, inventory_items, etc.)
- `{table_name}`: Target table name with _hist suffix for historical
- `created_at`: Historical incremental field
- `updated_at`: Incremental incremental field
- `{start_time}`: Default start time in ISO 8601 format

---

### 4. Load Config: `shopify_v1_customers_load.yml`

**EXACT TEMPLATE - Copy character-for-character:**

```yaml
in:
  type: shopify
  td_authentication_id: ${td_authentication_ids.shopify_v1.mccormick_flavors}
  target: ${datasource.name}
  incremental: true
  from_date: ${td.last_results.start_time}
  to_date: ${td.last_results.end_time}
  incremental_field: ${datasource.incremental_field}
  use_to_date: true

filters:
- type: add_time
  to_column:
    name: time
    type: timestamp
  from_value:
    mode: upload_time
- type: column
  add_columns:
    - {name: shop_flavor_flag, type: string, default: "SHOPIFY_MCCORMICKFLAVORS"}

out:
  mode: ${datasource.mode}
```

**Template Placeholders:**
- `${datasource.name}`: From datasources.yml
- `${datasource.incremental_field}`: From datasources.yml
- `${datasource.mode}`: From datasources.yml
- `shop_flavor_flag`: Custom column for shop identification

---

## Adding New Shopify v1 Objects

### Step 1: Add to Datasources Config

Add to both `hist_datasources` and `inc_datasources` in `shopify_v1_datasources.yml`:

**Historical:**
```yaml
  - name: {object}
    table_name: shopify_{object}_hist
    config_file: shopify_v1_{object}_load.yml
    incremental_field: created_at
    default_start_time: "2023-09-01T00:00:00.000Z"
    workflow_name: shopify_v1_ingest_hist.dig
    mode: append
```

**Incremental:**
```yaml
  - name: {object}
    table_name: shopify_{object}
    config_file: shopify_v1_{object}_load.yml
    incremental_field: updated_at
    default_start_time: "2025-09-01T00:00:00.000Z"
    workflow_name: shopify_v1_ingest_inc.dig
    mode: append
```

### Step 2: Create Load Config

File: `ingestion/config/shopify_v1_{object}_load.yml`

```yaml
in:
  type: shopify
  td_authentication_id: ${td_authentication_ids.shopify_v1.mccormick_flavors}
  target: ${datasource.name}
  incremental: true
  from_date: ${td.last_results.start_time}
  to_date: ${td.last_results.end_time}
  incremental_field: ${datasource.incremental_field}
  use_to_date: true

filters:
- type: add_time
  to_column:
    name: time
    type: timestamp
  from_value:
    mode: upload_time
- type: column
  add_columns:
    - {name: shop_flavor_flag, type: string, default: "SHOPIFY_MCCORMICKFLAVORS"}

out:
  mode: ${datasource.mode}
```

### Step 3: No Workflow Changes Needed

Workflows automatically process all datasources from config file.

---

## Timestamp Format

### Exact SQL Functions

**For incremental time calculation (ISO 8601 with .000Z):**

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

**For historical batch conversion:**

```sql
SELECT
  replace('${date_range.start_time}', '.000000', '.000Z') as start_time,
  replace('${date_range.end_time}', '.000000', '.000Z') as end_time
```

**NEVER modify** these timestamp functions. They are exact and tested.

---

## Credentials Configuration

### Required Entries in `database.yml`

```yaml
td_authentication_ids:
  shopify_v1:
    mccormick_flavors: 360617
```

### Authentication Setup

Configure Shopify v1 authentication in TD Console:
1. Navigate to Integrations Hub
2. Select Shopify v1 connector
3. Authenticate with Shopify account
4. Note the authentication ID
5. Update `database.yml` with the auth ID

---

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| "401 Unauthorized" | Invalid auth ID | Verify `shopify_v1.mccormick_flavors` auth ID in database.yml |
| "Invalid timestamp" | Wrong format | Use exact SQL functions, don't modify |
| "No data ingested" | Wrong incremental field | Historical uses `created_at`, incremental uses `updated_at` |
| "Duplicate records" | Batch already processed | Check ingestion_log for SUCCESS status |
| "Rate limit exceeded" | Too many parallel jobs | Reduce parallel limit from 3 to 2 |

### Debugging Steps

1. **Verify auth ID:**
   ```sql
   -- Check in database.yml
   SELECT '${td_authentication_ids.shopify_v1.mccormick_flavors}' as auth_id
   ```

2. **Check incremental times:**
   ```sql
   SELECT
     MAX(updated_at) as last_update,
     MIN(updated_at) as first_update
   FROM client_src.shopify_customers
   ```

3. **Check batch processing status:**
   ```sql
   SELECT
     source_name,
     table_name,
     status,
     start_time,
     end_time,
     records_processed
   FROM client_src.ingestion_log
   WHERE workflow_name = 'shopify_v1_ingest'
   ORDER BY time DESC
   LIMIT 20
   ```

---

## Historical Date Ranges

Uses shared `hist_date_ranges.yml` for monthly batch processing. See `workflow-patterns.md` for details.

---

## Testing Checklist

- [ ] Auth ID configured in `database.yml`
- [ ] Datasources defined for both hist and inc
- [ ] Incremental fields correct: `created_at` (hist), `updated_at` (inc)
- [ ] Load configs created for each object
- [ ] Custom column `shop_flavor_flag` included
- [ ] Timestamp format uses exact SQL functions
- [ ] Workflow syntax checked: `td wf check shopify_v1_ingest_inc.dig`
- [ ] Historical syntax checked: `td wf check shopify_v1_ingest_hist.dig`
- [ ] Test run completed: `td wf run shopify_v1_ingest_inc.dig`
- [ ] Data verified in target tables
- [ ] Logging verified in ingestion_log table

---

## Quick Reference

### Workflow Pattern (Incremental)

```
shopify_v1_ingest_inc.dig
└── load_incremental_data (parallel: limit 3)
    ├── log_ingestion_start
    ├── setup_table_and_time
    │   ├── create_incremental_table
    │   └── get_last_incremental_time (ISO 8601 .000Z)
    ├── load_incremental
    └── log_ingestion_success
```

### Workflow Pattern (Historical)

```
shopify_v1_ingest_hist.dig
└── load_historical_data (parallel: unlimited)
    ├── log_ingestion_start
    ├── setup_table
    └── load_monthly_batches (parallel: limit 3)
        ├── check_batch_status
        └── process_batch (if not already processed)
            ├── set_batch_time_range
            ├── log_batch_start
            ├── load_batch
            └── log_batch_success
```

---

## Migration to Shopify v2

If migrating to Shopify v2:
1. Read `shopify-v2.md` documentation
2. Update `type: shopify` → `type: shopify_v2`
3. Update auth ID to `shopify_v2.default`
4. Remove `shop_flavor_flag` custom column (if not needed)
5. Update `use_to_date: true` → remove (default in v2)
6. Test thoroughly before deploying

---

## Remember

✅ **DO:**
- Use exact templates as shown
- Use `created_at` for historical, `updated_at` for incremental
- Include `shop_flavor_flag` custom column
- Use exact timestamp SQL functions
- Check batch processing status to avoid duplicates

❌ **DON'T:**
- Mix `created_at` and `updated_at` in same workflow
- Modify timestamp conversion functions
- Remove `shop_flavor_flag` without understanding impact
- Skip batch status check in historical workflow
- Use Shopify v1 for new implementations (use v2 instead)

---

**When in doubt, copy the template exactly. These templates are production-tested.**
