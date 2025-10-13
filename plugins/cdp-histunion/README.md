# CDP Hist-Union Plugin

**Combine historical and incremental tables into unified tables with intelligent watermark management**

---

## Overview

The CDP Hist-Union plugin automates the consolidation of historical (full-load) and incremental tables into unified tables. It handles schema validation, watermark-based incremental loading, and special cases like full-reload tables with intelligent MCP-driven schema analysis.

### Purpose

Consolidate historical and incremental data sources by automating:
- UNION ALL logic between hist and inc tables
- Watermark management using inc_log table
- Schema validation and mismatch handling
- Full-load table detection (lists, metrics, etc.)
- Parallel execution for multiple tables
- NULL column handling for schema differences

---

## Features

### Intelligent Schema Validation
- **MCP Integration**: Real-time schema fetching from TD
- **Automatic Detection**: Identifies schema differences between hist/inc tables
- **NULL Handling**: Adds NULL columns for missing fields
- **Type Safety**: Ensures consistent data types across UNION

### Watermark Management
- **Automatic Tracking**: Uses `inc_log` table to track last processed time
- **Incremental Loading**: Only load new data since last run
- **Backfill Support**: Handle historical data reload
- **Time-based Filtering**: Efficient WHERE clauses on `time` column

### Special Table Handling
- **Full-Load Detection**: Automatically identifies tables that need full reload
  - `klaviyo_lists` - Always full load
  - `klaviyo_metric_data` - Always full load
  - `*_metrics` tables - Configurable
- **Insert Mode Selection**:
  - `APPEND` for incremental tables
  - `REPLACE` for full-load tables

### Schema Mismatch Resolution
- **Column Differences**: Handles when hist has columns inc doesn't (or vice versa)
- **NULL Padding**: Adds `NULL AS column_name` for missing columns
- **Order Preservation**: Maintains consistent column order
- **Type Casting**: Ensures compatible types across tables

### Parallel Execution
- **Batch Processing**: Process multiple tables concurrently
- **Independent Tasks**: Each table runs as parallel task
- **Resource Optimization**: Maximum throughput
- **Progress Tracking**: Individual task monitoring

---

## Slash Commands

### `/cdp-histunion:histunion-create`

Create hist-union for a single table pair.

**Usage:**
```bash
/cdp-histunion:histunion-create
```

**Prompts for:**
- Database name (e.g., `mck_src`)
- Base table name (e.g., `klaviyo_events`)
- Output table suffix (default: `_histunion`)
- Insert mode (append/replace)

**Generates:**
- `hist_union/queries/{table}_histunion.sql`
- `hist_union/{table}_histunion.dig` (optional workflow)

**Example:**
```
Input: database=mck_src, table=klaviyo_events
Checks for:
  - mck_src.klaviyo_events_hist
  - mck_src.klaviyo_events

Output:
  - mck_src.klaviyo_events_histunion
```

---

### `/cdp-histunion:histunion-batch`

Create hist-union for multiple tables in parallel with maximum efficiency.

**Usage:**
```bash
/cdp-histunion:histunion-batch
```

**Prompts for:**
- Database name
- List of base table names (comma-separated or line-separated)
- Common configuration

**Example Input:**
```
Database: mck_src

Tables:
klaviyo_events
klaviyo_profiles
klaviyo_campaigns
klaviyo_lists
shopify_orders
shopify_products
```

**Generates:**
- One SQL file per table in `hist_union/queries/`
- Master workflow with parallel execution: `hist_union/batch_histunion.dig`
- Logging and error handling for each table

**Batch Workflow:**
```yaml
# hist_union/batch_histunion.dig

timezone: UTC

_export:
  td:
    database: mck_src

_parallel: true

+histunion_klaviyo_events:
  td>: hist_union/queries/klaviyo_events_histunion.sql
  create_table: klaviyo_events_histunion

+histunion_klaviyo_profiles:
  td>: hist_union/queries/klaviyo_profiles_histunion.sql
  create_table: klaviyo_profiles_histunion

+histunion_shopify_orders:
  td>: hist_union/queries/shopify_orders_histunion.sql
  create_table: shopify_orders_histunion
```

---

### `/cdp-histunion:histunion-validate`

Validate generated SQL and workflows against production quality gates.

**Usage:**
```bash
/cdp-histunion:histunion-validate
```

**Validates:**
- SQL syntax correctness
- Table existence (hist and inc tables)
- Schema compatibility
- Watermark logic presence
- UNION ALL correctness
- NULL handling for schema mismatches
- Column order consistency
- Workflow syntax

**Output:**
```
Validating: hist_union/queries/klaviyo_events_histunion.sql

✓ SQL syntax valid
✓ Source tables exist:
  - mck_src.klaviyo_events_hist
  - mck_src.klaviyo_events
✓ Schema compatibility check passed
✓ Watermark logic found (inc_log table)
✓ UNION ALL structure correct
✓ Column count matches (45 columns)
✓ NULL handling for schema differences
✓ Incremental filter present
✓ Output table: mck_src.klaviyo_events_histunion

Overall: PASS
```

---

## SQL Patterns

### Pattern 1: Basic Hist-Union (Same Schema)

```sql
-- File: hist_union/queries/shopify_orders_histunion.sql

INSERT INTO mck_src.shopify_orders_histunion
SELECT * FROM mck_src.shopify_orders_hist

UNION ALL

SELECT * FROM mck_src.shopify_orders
WHERE time > (
    SELECT COALESCE(MAX(last_timestamp), 0)
    FROM inc_log
    WHERE table_name = 'shopify_orders'
)
```

### Pattern 2: Schema Mismatch Handling

```sql
-- Historical table has: id, name, email, created_at
-- Incremental table has: id, name, email, created_at, updated_at

INSERT INTO mck_src.customers_histunion

-- Historical data (missing updated_at column)
SELECT
    id,
    name,
    email,
    created_at,
    NULL AS updated_at,  -- Add NULL for missing column
    time
FROM mck_src.customers_hist

UNION ALL

-- Incremental data (has all columns)
SELECT
    id,
    name,
    email,
    created_at,
    updated_at,
    time
FROM mck_src.customers
WHERE time > (
    SELECT COALESCE(MAX(last_timestamp), 0)
    FROM inc_log
    WHERE table_name = 'customers'
)
```

### Pattern 3: Full-Load Table (Replace Mode)

```sql
-- File: hist_union/queries/klaviyo_lists_histunion.sql
-- Lists are always full snapshot, no incremental

INSERT OVERWRITE TABLE mck_src.klaviyo_lists_histunion
SELECT * FROM mck_src.klaviyo_lists
-- No UNION with hist, just use latest full snapshot
```

### Pattern 4: Complex Schema Differences

```sql
-- Incremental table has NEW column that historical doesn't

INSERT INTO mck_src.products_histunion

-- Historical (missing: discount_percent, tags array)
SELECT
    product_id,
    product_name,
    price,
    NULL AS discount_percent,        -- New column in incremental
    ARRAY[] AS tags,                  -- New array column
    created_at,
    time
FROM mck_src.products_hist

UNION ALL

-- Incremental (has all columns)
SELECT
    product_id,
    product_name,
    price,
    discount_percent,
    tags,
    created_at,
    time
FROM mck_src.products
WHERE time > (
    SELECT COALESCE(MAX(last_timestamp), 0)
    FROM inc_log
    WHERE table_name = 'products'
)
```

### Pattern 5: With Data Type Casting

```sql
INSERT INTO mck_src.events_histunion

SELECT
    event_id,
    CAST(timestamp AS BIGINT) AS timestamp,  -- Ensure consistent type
    user_id,
    event_type,
    CAST(properties AS JSON) AS properties,  -- Cast to JSON type
    time
FROM mck_src.events_hist

UNION ALL

SELECT
    event_id,
    CAST(timestamp AS BIGINT) AS timestamp,
    user_id,
    event_type,
    CAST(properties AS JSON) AS properties,
    time
FROM mck_src.events
WHERE time > (
    SELECT COALESCE(MAX(last_timestamp), 0)
    FROM inc_log
    WHERE table_name = 'events'
)
```

---

## Watermark Management

### Inc_Log Table Structure

```sql
CREATE TABLE IF NOT EXISTS inc_log (
    table_name VARCHAR,
    last_timestamp BIGINT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (table_name)
)
```

### Watermark Update

After successful hist-union:

```sql
INSERT INTO inc_log (table_name, last_timestamp, last_updated)
VALUES (
    'klaviyo_events',
    (SELECT MAX(time) FROM mck_src.klaviyo_events),
    CURRENT_TIMESTAMP
)
ON DUPLICATE KEY UPDATE
    last_timestamp = VALUES(last_timestamp),
    last_updated = VALUES(last_updated)
```

### Watermark Query Pattern

```sql
-- Get last processed timestamp
SELECT COALESCE(MAX(last_timestamp), 0)
FROM inc_log
WHERE table_name = 'table_name'

-- If no entry exists, COALESCE returns 0 (load all data)
-- Otherwise, returns last timestamp
```

---

## Usage Examples

### Example 1: Single Table Hist-Union

```bash
/cdp-histunion:histunion-create
```

**Input:**
- Database: `mck_src`
- Table: `klaviyo_events`
- Output suffix: `_histunion`
- Mode: Append

**MCP Analysis:**
```
Checking schema for:
  - mck_src.klaviyo_events_hist (45 columns)
  - mck_src.klaviyo_events (46 columns)

Schema Difference Detected:
  Incremental table has additional column: 'incremental_date'

Resolution:
  Add 'NULL AS incremental_date' to historical SELECT
```

**Generated SQL:**
```sql
INSERT INTO mck_src.klaviyo_events_histunion

SELECT
    event_id,
    profile_id,
    email,
    event_name,
    timestamp,
    properties,
    -- ... (other 38 columns)
    NULL AS incremental_date,  -- Missing in hist table
    time
FROM mck_src.klaviyo_events_hist

UNION ALL

SELECT
    event_id,
    profile_id,
    email,
    event_name,
    timestamp,
    properties,
    -- ... (other 38 columns)
    incremental_date,
    time
FROM mck_src.klaviyo_events
WHERE time > (
    SELECT COALESCE(MAX(last_timestamp), 0)
    FROM inc_log
    WHERE table_name = 'klaviyo_events'
)
```

---

### Example 2: Batch Hist-Union for E-commerce

```bash
/cdp-histunion:histunion-batch
```

**Input:**
```
Database: shopify_src

Tables:
products
customers
orders
inventory
discounts
collections
```

**Generated:**
- 6 SQL files (one per table)
- Parallel execution workflow
- Individual watermark tracking
- Consolidated logging

**Workflow:**
```yaml
_parallel: true

+histunion_products:
  td>: hist_union/queries/products_histunion.sql

+histunion_customers:
  td>: hist_union/queries/customers_histunion.sql

+histunion_orders:
  td>: hist_union/queries/orders_histunion.sql

+histunion_inventory:
  td>: hist_union/queries/inventory_histunion.sql

+histunion_discounts:
  td>: hist_union/queries/discounts_histunion.sql

+histunion_collections:
  td>: hist_union/queries/collections_histunion.sql
```

---

### Example 3: Full-Load Table (Klaviyo Lists)

```bash
/cdp-histunion:histunion-create
```

**Input:**
- Database: `mck_src`
- Table: `klaviyo_lists`

**Auto-Detection:**
```
Detected full-load table: klaviyo_lists
Using INSERT OVERWRITE mode (no watermark)
```

**Generated SQL:**
```sql
-- Full snapshot, no historical union needed
INSERT OVERWRITE TABLE mck_src.klaviyo_lists_histunion
SELECT * FROM mck_src.klaviyo_lists
```

---

## Special Cases

### Case 1: Missing Historical Table

**Scenario:** Only incremental table exists (no `_hist`)

**Solution:**
```sql
-- Just use incremental table
INSERT INTO mck_src.table_histunion
SELECT * FROM mck_src.table
WHERE time > (
    SELECT COALESCE(MAX(last_timestamp), 0)
    FROM inc_log
    WHERE table_name = 'table'
)
```

### Case 2: Missing Incremental Table

**Scenario:** Only historical table exists (no incremental yet)

**Solution:**
```sql
-- Just use historical table
INSERT INTO mck_src.table_histunion
SELECT * FROM mck_src.table_hist
```

### Case 3: Both Tables Missing Columns

**Scenario:** Each table has unique columns

**Solution:**
```sql
-- Hist has: id, name, created_at
-- Inc has: id, name, updated_at

INSERT INTO mck_src.table_histunion

SELECT
    id,
    name,
    created_at,
    NULL AS updated_at
FROM mck_src.table_hist

UNION ALL

SELECT
    id,
    name,
    NULL AS created_at,
    updated_at
FROM mck_src.table
WHERE time > (SELECT COALESCE(MAX(last_timestamp), 0) FROM inc_log WHERE table_name = 'table')
```

---

## Best Practices

### 1. Schema Validation
- Always use MCP to validate schemas before generation
- Don't assume hist and inc have same schema
- Test with sample data first
- Monitor schema changes over time

### 2. Watermark Management
- Ensure inc_log table exists before running
- Update watermark after successful load
- Handle watermark failures gracefully
- Monitor watermark progression

### 3. Performance Optimization
- Use parallel execution for multiple tables
- Schedule during off-peak hours for large tables
- Monitor query execution times
- Consider partitioning for very large tables

### 4. Error Handling
- Validate source tables exist before execution
- Handle schema mismatches explicitly
- Log all operations
- Set up alerts for failures

### 5. Testing
- Test with small time ranges first
- Verify row counts match expectations
- Check for duplicates in output
- Validate data types

---

## File Structure

```
hist_union/
├── queries/
│   ├── klaviyo_events_histunion.sql
│   ├── klaviyo_profiles_histunion.sql
│   ├── klaviyo_lists_histunion.sql
│   ├── shopify_orders_histunion.sql
│   └── shopify_products_histunion.sql
│
├── batch_histunion.dig
├── klaviyo_events_histunion.dig
└── validation_report.txt
```

---

## Common Issues

### Issue: Column Count Mismatch

**Error:** `UNION ALL requires same number of columns`

**Solution:**
```bash
# Re-run with MCP schema validation
/cdp-histunion:histunion-create
# Agent will automatically detect and add NULL columns
```

### Issue: Type Mismatch in UNION

**Error:** `Cannot union BIGINT and VARCHAR`

**Solution:**
```sql
-- Add explicit CAST in both SELECTs
SELECT CAST(column AS VARCHAR) ...
UNION ALL
SELECT CAST(column AS VARCHAR) ...
```

### Issue: Watermark Not Updating

**Cause:** inc_log table doesn't exist or permissions issue

**Solution:**
```sql
-- Create inc_log table
CREATE TABLE IF NOT EXISTS inc_log (
    table_name VARCHAR,
    last_timestamp BIGINT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)

-- Grant permissions
GRANT INSERT, UPDATE, SELECT ON inc_log TO workflow_user
```

### Issue: Duplicate Records in Output

**Cause:** Watermark not properly tracked

**Solution:**
- Verify watermark update logic
- Check inc_log table for correct timestamp
- Add DISTINCT if needed (temporary fix)

---

## Quality Gates

All hist-union SQL must include:

1. **UNION ALL**: Between hist and inc tables
2. **Watermark Filter**: WHERE time > watermark for inc table
3. **Schema Alignment**: Same column count and order
4. **NULL Handling**: For schema differences
5. **Type Consistency**: Matching data types across UNION
6. **Comments**: Document schema differences
7. **Table Check**: Verify source tables exist

---

## Monitoring

### Check Histunion Progress

```sql
-- Row counts
SELECT
    'hist' AS source,
    COUNT(*) AS row_count
FROM mck_src.klaviyo_events_hist

UNION ALL

SELECT
    'inc' AS source,
    COUNT(*) AS row_count
FROM mck_src.klaviyo_events

UNION ALL

SELECT
    'histunion' AS source,
    COUNT(*) AS row_count
FROM mck_src.klaviyo_events_histunion
```

### Verify Incremental Loading

```sql
-- Check watermark
SELECT *
FROM inc_log
WHERE table_name = 'klaviyo_events'

-- Check latest data in histunion
SELECT MAX(time) AS latest_time
FROM mck_src.klaviyo_events_histunion
```

---

## Support

For assistance:
- Review generated SQL in `hist_union/queries/`
- Run validation command
- Check MCP schema analysis
- Verify source table existence

---

**Version:** 1.4.0
**Last Updated:** 2024-10-13
**Maintained by:** APS CDP Team
