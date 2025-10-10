# Shopify v2 Ingestion - Exact Templates

## Overview
Shopify v2 connector handles e-commerce data including products and product variants. This source has separate workflows for historical and incremental modes, with different incremental fields for each mode.

## Source Characteristics

| Characteristic | Value |
|---------------|-------|
| **Connector Type** | `shopify_v2` |
| **Parallel Processing** | Limited (`_parallel: {limit: 3}`) |
| **Timestamp Format** | ISO 8601 (`.000Z`) |
| **Incremental Fields** | `created_at` (hist), `updated_at` (inc) |
| **Success Logging** | `log_ingestion_success.sql` (with time tracking) |
| **Data Sources** | Products, Product Variants |

---

## Workflow Files

### Incremental Workflow

**File**: `shopify_v2_ingest_inc.dig`

**EXACT TEMPLATE**:
```yaml
timezone: UTC

_export:
  !include : config/database.yml
  !include : config/shopify_v2_datasources.yml
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
      source_name: 'shopify'
      workflow_name: 'shopify_v2_ingest'
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
      workflow_name: 'shopify_v2_ingest'
      table_name: ${datasource.table_name}

_error:
  +log_ingestion_error:
    td>: sql/log_ingestion_error.sql
    database: ${databases.src}
    source_name: 'shopify'
    workflow_name: 'shopify_ingest'
    error_message: 'shopify v2 ingestion failed. check shopify_v2_ingest'
```

### Historical Workflow

**File**: `shopify_v2_ingest_hist.dig`

Structure identical to incremental, but uses `${hist_datasources}` instead of `${inc_datasources}`.

---

## Datasources Configuration

**File**: `config/shopify_v2_datasources.yml`

**EXACT TEMPLATE**:
```yaml
hist_datasources:
  - name: products
    table_name: shopify_products_hist
    config_file: shopify_v2_products_load.yml
    incremental_field: created_at
    default_start_time: "2023-01-01T00:00:00.000Z"
    workflow_name: shopify_v2_ingest_hist.dig
    mode: append

  - name: product_variants
    table_name: shopify_product_variants_hist
    config_file: shopify_v2_product_variants_load.yml
    incremental_field: created_at
    default_start_time: "2023-01-01T00:00:00.000Z"
    workflow_name: shopify_v2_ingest_hist.dig
    mode: append

inc_datasources:
  - name: products
    table_name: shopify_products
    config_file: shopify_v2_products_load.yml
    incremental_field: updated_at
    default_start_time: "2025-01-01T00:00:00.000Z"
    workflow_name: shopify_v2_ingest_inc.dig
    mode: append

  - name: product_variants
    table_name: shopify_product_variants
    config_file: shopify_v2_product_variants_load.yml
    incremental_field: updated_at
    default_start_time: "2025-01-01T00:00:00.000Z"
    workflow_name: shopify_v2_ingest_inc.dig
    mode: append
```

**CRITICAL**: Historical uses `created_at`, Incremental uses `updated_at`

---

## Load Configuration Files

### Products

**File**: `config/shopify_v2_products_load.yml`

**EXACT TEMPLATE**:
```yaml
in:
  type: shopify_v2
  td_authentication_id: 360360
  target: products
  incremental: true
  from_date: ${td.last_results.start_time}
  to_date: ${td.last_results.end_time}
  incremental_field: ${datasource.incremental_field}
  include_image: true

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

### Product Variants

**File**: `config/shopify_v2_product_variants_load.yml`

**EXACT TEMPLATE**:
```yaml
in:
  type: shopify_v2
  td_authentication_id: 360360
  target: product_variants
  incremental: true
  from_date: ${td.last_results.start_time}
  to_date: ${td.last_results.end_time}
  incremental_field: ${datasource.incremental_field}
  include_image: true

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

## Critical Shopify v2 Patterns

### 1. Historical vs. Incremental Field Difference

**CRITICAL**: Shopify v2 uses DIFFERENT fields for historical vs. incremental

| Mode | Field Used | Purpose |
|------|-----------|---------|
| **Historical** | `created_at` | Get all products created in time range |
| **Incremental** | `updated_at` | Get only products updated since last run |

**Why**: Historical backfill needs creation dates, incremental updates need modification dates.

---

### 2. Shopify v2 Timestamp Functions

**Start Time** (Get Max from Table):
```sql
replace(TD_TIME_STRING(TD_TIME_PARSE(max(${datasource.incremental_field})), 's!'), ' ', 'T') || '.000Z'
```

**End Time** (Current Timestamp):
```sql
to_iso8601(current_timestamp)
```

**Format**: ISO 8601 standard (`.000Z` with 3 decimals, WITH Z)

**Example Output**:
- Start: `2023-01-01T00:00:00.000Z`
- End: `2025-10-10T13:45:30.123Z`

---

### 3. Target Parameter

```yaml
target: products  # or product_variants
```

The `target` parameter specifies which Shopify object to fetch. Common targets:
- `products`
- `product_variants`
- `customers` (v1 only)
- `orders` (v1 only)
- `inventory_items` (v1 only)

---

### 4. Include Image

```yaml
include_image: true
```

Fetches product images along with product data. Set to `false` if images not needed (faster processing).

---

### 5. Authentication

**Authentication ID**: `360360`

Configured in Treasure Data console under Authentications for Shopify.

**Required Secrets** (in `credentials_ingestion.json`):
```json
{
  "shopify_shop_name": "your-shop-name",
  "shopify_api_password": "your_api_password"
}
```

**Note**: Current implementation uses `td_authentication_id` instead of secrets directly.

---

## Adding New Shopify v2 Data Sources

### Step 1: Verify Target Name

Check Shopify v2 connector documentation for supported targets.

**Current targets**:
- `products`
- `product_variants`

### Step 2: Add to Datasources Config

Add to both `hist_datasources` and `inc_datasources` in `config/shopify_v2_datasources.yml`:

**Historical**:
```yaml
hist_datasources:
  - name: [target_name]
    table_name: shopify_[target_name]_hist
    config_file: shopify_v2_[target_name]_load.yml
    incremental_field: created_at  # Historical uses created_at
    default_start_time: "2023-01-01T00:00:00.000Z"
    workflow_name: shopify_v2_ingest_hist.dig
    mode: append
```

**Incremental**:
```yaml
inc_datasources:
  - name: [target_name]
    table_name: shopify_[target_name]
    config_file: shopify_v2_[target_name]_load.yml
    incremental_field: updated_at  # Incremental uses updated_at
    default_start_time: "2025-01-01T00:00:00.000Z"
    workflow_name: shopify_v2_ingest_inc.dig
    mode: append
```

### Step 3: Create Load Config

Create `config/shopify_v2_[target_name]_load.yml`:

```yaml
in:
  type: shopify_v2
  td_authentication_id: 360360
  target: [target_name]
  incremental: true
  from_date: ${td.last_results.start_time}
  to_date: ${td.last_results.end_time}
  incremental_field: ${datasource.incremental_field}
  include_image: true  # Set to false if images not needed

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

**Incremental**:
```bash
td wf run shopify_v2_ingest_inc.dig
```

**Historical**:
```bash
td wf run shopify_v2_ingest_hist.dig
```

---

## Common Shopify v2 Issues

### Issue: "Invalid timestamp format"
**Cause**: Missing `.000Z` suffix or wrong format

**Solution**:
1. Verify using ISO 8601 format with `.000Z`
2. Use exact timestamp functions from this document
3. Check default_start_time has correct format

### Issue: No data in historical mode
**Cause**: Using `updated_at` instead of `created_at`

**Solution**: Historical datasources MUST use `incremental_field: created_at`

### Issue: Missing updates in incremental mode
**Cause**: Using `created_at` instead of `updated_at`

**Solution**: Incremental datasources MUST use `incremental_field: updated_at`

### Issue: "401 Unauthorized"
**Cause**: Invalid Shopify API credentials

**Solution**:
1. Verify `td_authentication_id: 360360` is correct
2. Check authentication in TD console has valid credentials
3. Ensure shop name and API password are correct

### Issue: Large payload / slow processing
**Cause**: Including images for large product catalogs

**Solution**: Set `include_image: false` if images not needed

### Issue: Rate limit exceeded
**Cause**: Too many parallel requests

**Solution**: Shopify v2 already uses `_parallel: {limit: 3}` - if still hitting limits, reduce to `limit: 2` or `limit: 1`

---

## Performance Optimization

### 1. Parallel Processing
```yaml
_parallel:
  limit: 3  # Respects Shopify API rate limits
```

### 2. Image Inclusion
```yaml
include_image: false  # Faster if images not needed
```

### 3. Time Range Optimization
Use appropriate `default_start_time` to avoid loading excessive historical data.

### 4. Separate Historical and Incremental
Run historical once for backfill, then use incremental for ongoing updates.

---

## Workflow Execution Strategy

### Initial Setup (Historical Backfill)
1. Run `shopify_v2_ingest_hist.dig` to load all historical data
2. Uses `created_at` to get all products since inception
3. One-time operation (or when new products added before current monitoring period)

### Ongoing Updates (Incremental)
1. Run `shopify_v2_ingest_inc.dig` daily/hourly
2. Uses `updated_at` to get only modified products
3. Much faster and lighter than historical

---

## Critical Reminders

1. **ALWAYS use ISO 8601 format** (`.000Z` with 3 decimals)
2. **ALWAYS use `created_at` for historical**, `updated_at` for incremental
3. **ALWAYS use `target` parameter** to specify Shopify object
4. **REMEMBER**: Historical and incremental use DIFFERENT fields
5. **REMEMBER**: `from_date` and `to_date` come from td.last_results
6. **USE `_parallel: {limit: 3}`** to respect API rate limits
7. **SET `include_image: true`** only if images needed
8. **VERIFY** td_authentication_id matches your Shopify auth
9. **TEST** incremental mode first before running historical backfill
10. **SEPARATE** historical and incremental workflows (don't combine)

---

## Data Source Reference

| Target | Historical Field | Incremental Field | Include Image? | Notes |
|--------|-----------------|-------------------|----------------|-------|
| products | created_at | updated_at | Optional | Product master data |
| product_variants | created_at | updated_at | Optional | Product SKU variations |

---

## Shopify v1 vs. v2

**Note**: This project has legacy Shopify v1 workflows (`shopify_v1_ingest_inc.dig`, `shopify_v1_ingest_hist.dig`).

**Use v2** for:
- New implementations
- Better performance
- More reliable incremental processing

**v1 legacy workflows** exist for:
- `customers`
- `inventory_items`

These should eventually be migrated to v2 or alternative connectors.

---

## Reference

**Related Documentation**:
- Timestamp formats: `docs/patterns/timestamp-formats.md`
- Workflow patterns: `docs/patterns/workflow-patterns.md`
- Logging patterns: `docs/patterns/logging-patterns.md`
- Incremental patterns: `docs/patterns/incremental-patterns.md`

**Connector Documentation**:
- [Treasure Data Shopify v2 Connector](https://docs.treasuredata.com/display/public/PD/Shopify+Import+Integration)
