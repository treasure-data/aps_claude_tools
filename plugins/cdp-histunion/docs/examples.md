# CDP Hist-Union Plugin - Examples

## Overview
This document provides real-world examples of using the CDP hist-union plugin to create workflows that combine historical and incremental table data.

---

## Example 1: Simple Table with Identical Schemas

### Scenario
Create hist-union for a table where incremental and historical tables have identical schemas.

### Input
```
Table: client_src.klaviyo_campaigns
Inc schema: id, name, status, created_at, updated_at, time
Hist schema: id, name, status, created_at, updated_at, time
```

### Generated SQL (queries/klaviyo_campaigns.sql)
```sql
-- Create histunion table if not exists
CREATE TABLE IF NOT EXISTS client_src.klaviyo_campaigns_histunion (
  id varchar,
  name varchar,
  status varchar,
  created_at varchar,
  updated_at varchar,
  time bigint
);

-- Insert incremental data from both hist and inc tables
INSERT INTO client_src.klaviyo_campaigns_histunion
-- Historical data
SELECT
  id,
  name,
  status,
  created_at,
  updated_at,
  time
FROM client_src.klaviyo_campaigns_hist
WHERE time > COALESCE(
  (SELECT MAX(inc_value) FROM client_config.inc_log
   WHERE table_name = 'klaviyo_campaigns_hist' AND project_name = 'hist_union'),
  0
)

UNION ALL

-- Incremental data
SELECT
  id,
  name,
  status,
  created_at,
  updated_at,
  time
FROM client_src.klaviyo_campaigns
WHERE time > COALESCE(
  (SELECT MAX(inc_value) FROM client_config.inc_log
   WHERE table_name = 'klaviyo_campaigns' AND project_name = 'hist_union'),
  0
);

-- Update watermark for historical table
INSERT INTO client_config.inc_log
SELECT 'klaviyo_campaigns_hist' table_name, 'hist_union' project_name, MAX(time) inc_value
FROM client_src.klaviyo_campaigns_hist;

-- Update watermark for incremental table
INSERT INTO client_config.inc_log
SELECT 'klaviyo_campaigns' table_name, 'hist_union' project_name, MAX(time) inc_value
FROM client_src.klaviyo_campaigns;
```

---

## Example 2: Table with Schema Differences

### Scenario
Create hist-union for a table where incremental table has an extra `incremental_date` column.

### Input
```
Table: client_src.klaviyo_events
Inc schema: incremental_date, id, event_name, user_id, timestamp, properties, time
Hist schema: id, event_name, user_id, timestamp, properties, time
```

### Generated SQL (queries/klaviyo_events.sql)
```sql
-- Create histunion table if not exists
-- **CRITICAL**: Use INCREMENTAL table schema as base (includes incremental_date column)
CREATE TABLE IF NOT EXISTS client_src.klaviyo_events_histunion (
  incremental_date varchar,
  id varchar,
  event_name varchar,
  user_id varchar,
  timestamp varchar,
  properties varchar,
  time bigint
);

-- Insert incremental data from both hist and inc tables
INSERT INTO client_src.klaviyo_events_histunion
-- Historical data (add NULL for incremental_date since hist table doesn't have it)
SELECT
  NULL as incremental_date,
  id,
  event_name,
  user_id,
  timestamp,
  properties,
  time
FROM client_src.klaviyo_events_hist
WHERE time > COALESCE(
  (SELECT MAX(inc_value) FROM client_config.inc_log
   WHERE table_name = 'klaviyo_events_hist' AND project_name = 'hist_union'),
  0
)

UNION ALL

-- Incremental data (use all columns including incremental_date)
SELECT
  incremental_date,
  id,
  event_name,
  user_id,
  timestamp,
  properties,
  time
FROM client_src.klaviyo_events
WHERE time > COALESCE(
  (SELECT MAX(inc_value) FROM client_config.inc_log
   WHERE table_name = 'klaviyo_events' AND project_name = 'hist_union'),
  0
);

-- Update watermark for historical table
INSERT INTO client_config.inc_log
SELECT 'klaviyo_events_hist' table_name, 'hist_union' project_name, MAX(time) inc_value
FROM client_src.klaviyo_events_hist;

-- Update watermark for incremental table
INSERT INTO client_config.inc_log
SELECT 'klaviyo_events' table_name, 'hist_union' project_name, MAX(time) inc_value
FROM client_src.klaviyo_events;
```

---

## Example 3: Full Load Table

### Scenario
Create hist-union for a full load table (klaviyo_lists) that requires complete reload every run.

### Input
```
Table: client_src.klaviyo_lists
Type: FULL LOAD (complete reload each run)
Inc schema: id, name, list_type, created, updated, time
Hist schema: id, name, list_type, created, updated, time
```

### Generated SQL (queries/klaviyo_lists.sql)
```sql
-- Drop and recreate histunion table for full load
DROP TABLE IF EXISTS client_src.klaviyo_lists_histunion;

CREATE TABLE client_src.klaviyo_lists_histunion (
  id varchar,
  name varchar,
  list_type varchar,
  created varchar,
  updated varchar,
  time bigint
);

-- Full load from both hist and inc tables (NO WHERE CLAUSE)
INSERT INTO client_src.klaviyo_lists_histunion
-- Historical data
SELECT
  id,
  name,
  list_type,
  created,
  updated,
  time
FROM client_src.klaviyo_lists_hist

UNION ALL

-- Incremental data
SELECT
  id,
  name,
  list_type,
  created,
  updated,
  time
FROM client_src.klaviyo_lists;

-- Update watermark for historical table
INSERT INTO client_config.inc_log
SELECT 'klaviyo_lists_hist' table_name, 'hist_union' project_name, MAX(time) inc_value
FROM client_src.klaviyo_lists_hist;

-- Update watermark for incremental table
INSERT INTO client_config.inc_log
SELECT 'klaviyo_lists' table_name, 'hist_union' project_name, MAX(time) inc_value
FROM client_src.klaviyo_lists;
```

---

## Example 4: Batch Processing Multiple Tables

### Scenario
Create hist-union workflows for multiple tables at once.

### Input
```
Tables:
1. client_src.klaviyo_events
2. client_src.shopify_products
3. client_src.onetrust_profiles
4. client_src.klaviyo_lists (full load)
```

### Generated Workflow (hist_union_runner.dig)
```yaml
timezone: UTC

_export:
  td:
    database: client_src
  lkup_db: client_config

+create_inc_log_table:
  td>:
  query: |
    CREATE TABLE IF NOT EXISTS ${lkup_db}.inc_log (
      table_name varchar,
      project_name varchar,
      inc_value bigint
    )

+hist_union_tasks:
  _parallel: true

  +klaviyo_events_histunion:
    td>: queries/klaviyo_events.sql

  +shopify_products_histunion:
    td>: queries/shopify_products.sql

  +onetrust_profiles_histunion:
    td>: queries/onetrust_profiles.sql

  +klaviyo_lists_histunion:
    td>: queries/klaviyo_lists.sql
```

### Files Created
```
hist_union/
├── hist_union_runner.dig
└── queries/
    ├── klaviyo_events.sql
    ├── shopify_products.sql
    ├── onetrust_profiles.sql
    └── klaviyo_lists.sql
```

---

## Example 5: Reserved Keywords Handling

### Scenario
Table has a column named "index" which is a reserved keyword in Presto/Trino.

### Input
```
Table: client_src.product_catalog
Inc schema: id, "index", name, category, price, time
Hist schema: id, "index", name, category, price, time
```

### Generated SQL (queries/product_catalog.sql)
```sql
-- Create histunion table if not exists
CREATE TABLE IF NOT EXISTS client_src.product_catalog_histunion (
  id varchar,
  "index" bigint,
  name varchar,
  category varchar,
  price double,
  time bigint
);

-- Insert incremental data from both hist and inc tables
INSERT INTO client_src.product_catalog_histunion
-- Historical data
SELECT
  id,
  "index",
  name,
  category,
  price,
  time
FROM client_src.product_catalog_hist
WHERE time > COALESCE(
  (SELECT MAX(inc_value) FROM client_config.inc_log
   WHERE table_name = 'product_catalog_hist' AND project_name = 'hist_union'),
  0
)

UNION ALL

-- Incremental data
SELECT
  id,
  "index",
  name,
  category,
  price,
  time
FROM client_src.product_catalog
WHERE time > COALESCE(
  (SELECT MAX(inc_value) FROM client_config.inc_log
   WHERE table_name = 'product_catalog' AND project_name = 'hist_union'),
  0
);

-- Update watermarks
INSERT INTO client_config.inc_log
SELECT 'product_catalog_hist' table_name, 'hist_union' project_name, MAX(time) inc_value
FROM client_src.product_catalog_hist;

INSERT INTO client_config.inc_log
SELECT 'product_catalog' table_name, 'hist_union' project_name, MAX(time) inc_value
FROM client_src.product_catalog;
```

**Note**: Double quotes `"index"` are used instead of backticks for Presto/Trino compatibility.

---

## Example 6: Custom Lookup Database

### Scenario
Use a custom database for inc_log watermark tracking instead of default client_config.

### Input
```
Table: mc_src.users
Lookup DB: mc_config (user-specified)
```

### Generated Workflow
```yaml
timezone: UTC

_export:
  td:
    database: mc_src
  lkup_db: mc_config

+create_inc_log_table:
  td>:
  query: |
    CREATE TABLE IF NOT EXISTS ${lkup_db}.inc_log (
      table_name varchar,
      project_name varchar,
      inc_value bigint
    )

+hist_union_tasks:
  _parallel: true

  +users_histunion:
    td>: queries/users.sql
```

### Generated SQL
```sql
-- Uses mc_config.inc_log instead of client_config.inc_log
WHERE time > COALESCE(
  (SELECT MAX(inc_value) FROM mc_config.inc_log
   WHERE table_name = 'users_hist' AND project_name = 'hist_union'),
  0
)
```

---

## Verification Queries

### Check Watermarks
```sql
SELECT
  table_name,
  project_name,
  inc_value,
  FROM_UNIXTIME(inc_value) as last_update_time
FROM client_config.inc_log
WHERE project_name = 'hist_union'
ORDER BY table_name;
```

### Compare Row Counts
```sql
-- Check individual table counts
SELECT 'inc' as source, COUNT(*) as row_count FROM client_src.klaviyo_events
UNION ALL
SELECT 'hist', COUNT(*) FROM client_src.klaviyo_events_hist
UNION ALL
SELECT 'histunion', COUNT(*) FROM client_src.klaviyo_events_histunion;
```

### Verify Schema
```sql
-- Check histunion table structure
SHOW COLUMNS FROM client_src.klaviyo_events_histunion;
```

### Sample Data Validation
```sql
-- Check for NULL values in incremental_date (should only be from hist table)
SELECT
  CASE WHEN incremental_date IS NULL THEN 'from_hist' ELSE 'from_inc' END as source,
  COUNT(*) as row_count
FROM client_src.klaviyo_events_histunion
GROUP BY 1;
```

---

## Common Patterns

### Pattern 1: Initial Run (No Watermarks)
- First run will process ALL data from both hist and inc tables
- COALESCE defaults to 0, so WHERE time > 0 includes all records
- Watermarks are established after first successful run

### Pattern 2: Subsequent Runs (With Watermarks)
- Only processes new data since last watermark
- Hist table watermark tracks highest time from hist table
- Inc table watermark tracks highest time from inc table
- Each table processed independently

### Pattern 3: Full Load Refresh
- Drops existing histunion table completely
- Recreates with fresh data from both sources
- No incremental filtering (all data loaded every time)
- Watermarks updated but not used for filtering

---

## Troubleshooting Examples

### Issue 1: Column Count Mismatch
```
Error: Column count mismatch in UNION ALL
```

**Cause**: Inc and hist tables have different number of columns, but NULL not added for missing column.

**Fix**: Add NULL for columns in inc but not in hist.

### Issue 2: Reserved Keyword Error
```
Error: syntax error at or near "index"
```

**Cause**: Using backticks or no quotes for reserved keyword.

**Fix**: Use double quotes: `"index"` instead of `` `index` ``

### Issue 3: Watermark Not Updating
```
Issue: Same data processed repeatedly
```

**Cause**: Watermark INSERT not present or project_name incorrect.

**Fix**: Ensure both watermark INSERTs present with project_name = 'hist_union'

---

## Performance Tips

### Tip 1: Parallel Execution
```yaml
# Process multiple tables concurrently
+hist_union_tasks:
  _parallel: true  # Enable parallel processing
```

### Tip 2: Watermark Indexing
```sql
-- Create index on inc_log for faster lookups
CREATE INDEX IF NOT EXISTS idx_inc_log_lookup
ON client_config.inc_log (table_name, project_name);
```

### Tip 3: Partition Large Tables
```sql
-- For very large histunion tables, consider partitioning
CREATE TABLE client_src.klaviyo_events_histunion (
  ...
)
PARTITIONED BY (dt);
```

---

These examples demonstrate the flexibility and robustness of the CDP hist-union plugin in handling various real-world scenarios.
