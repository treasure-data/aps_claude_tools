---
name: histunion-create
description: Create hist-union workflow for combining historical and incremental table data
---

# Create Hist-Union Workflow

## ⚠️ CRITICAL: This command enforces strict schema validation and template adherence

I'll help you create a production-ready hist-union workflow to combine historical and incremental table data.

---

## Required Information

Please provide the following details:

### 1. Table Names
You can provide table names in any of these formats:
- **Base name**: `mck_src.klaviyo_events` (I'll derive hist and histunion names)
- **Hist name**: `mck_src.klaviyo_events_hist` (I'll derive inc and histunion names)
- **Explicit**: Inc: `mck_src.klaviyo_events`, Hist: `mck_src.klaviyo_events_hist`

### 2. Lookup Database (Optional)
- **Lookup/Config Database**: Database for inc_log watermark table
- **Default**: `mck_references` (will be used if not specified)

---

## What I'll Do

### Step 1: Parse Table Names Intelligently
I will automatically derive all three table names:
```
From your input, I'll extract:
- Database name
- Base table name (removing _hist or _histunion if present)
- Inc table: {database}.{base_name}
- Hist table: {database}.{base_name}_hist
- Target table: {database}.{base_name}_histunion
```

### Step 2: Get Exact Schemas via MCP Tool (MANDATORY)
I will use MCP tool to get exact column information:
```
1. Call mcp__mcc_treasuredata__describe_table for inc table
   - Get complete column list
   - Get exact column order
   - Get data types

2. Call mcp__mcc_treasuredata__describe_table for hist table
   - Get complete column list
   - Get exact column order
   - Get data types

3. Compare schemas:
   - Identify columns in inc but not in hist
   - Identify any schema differences
   - Document column order
```

### Step 3: Check Full Load Status
I will check if table requires full load processing:
```
IF table_name IN ('klaviyo_lists', 'klaviyo_metric_data'):
    Use FULL LOAD template (Case 3)
    - DROP TABLE and recreate
    - Load ALL data (no WHERE clause)
    - Still update watermarks
ELSE:
    Use INCREMENTAL template (Case 1 or 2)
    - CREATE TABLE IF NOT EXISTS
    - Filter using inc_log watermarks
    - Update watermarks after insert
```

### Step 4: Select Correct SQL Template
Based on schema comparison:
```
IF full_load_table:
    Template = Case 3 (Full Load)
ELIF inc_schema == hist_schema:
    Template = Case 1 (Identical schemas)
ELSE:
    Template = Case 2 (Inc has extra columns)
```

### Step 5: Generate SQL File
I will create SQL file with exact schema:
```
File: hist_union/queries/{base_table_name}.sql

Structure:
- CREATE TABLE (or DROP + CREATE for full load)
  - Use EXACT inc table schema
  - Maintain exact column order

- INSERT INTO with UNION ALL:
  - Historical SELECT
    - Add NULL for columns missing in hist
    - Use inc_log watermark (skip for full load)
  - Incremental SELECT
    - Use all columns in exact order
    - Use inc_log watermark (skip for full load)

- UPDATE watermarks:
  - Update hist table watermark
  - Update inc table watermark
```

### Step 6: Create or Update Digdag Workflow
I will update the workflow file:
```
File: hist_union/hist_union_runner.dig

If file doesn't exist, create with:
- timezone: UTC
- _export section with database and lkup_db
- +create_inc_log_table task
- +hist_union_tasks section with _parallel: true

Add new task:
+hist_union_tasks:
  _parallel: true
  +{table_name}_histunion:
    td>: queries/{table_name}.sql
```

### Step 7: Verify Quality Gates
Before delivering, I will verify:
```
✅ MCP tool used for both inc and hist table schemas
✅ Schema differences identified and documented
✅ Correct template selected (Case 1, 2, or 3)
✅ All inc table columns present in CREATE TABLE
✅ Exact column order maintained from inc schema
✅ NULL added for columns missing in hist table (if applicable)
✅ Watermark updates present for both hist and inc tables
✅ _parallel: true configured for concurrent execution
✅ No schedule block in workflow file
✅ Correct lkup_db set (mck_references or user-specified)
```

---

## Output

I will generate:

### For Single Table:
1. **hist_union/queries/{table_name}.sql** - SQL for combining hist and inc data
2. **hist_union/hist_union_runner.dig** - Updated workflow file

### File Contents:

**SQL File Structure:**
```sql
-- CREATE TABLE using exact inc table schema
CREATE TABLE IF NOT EXISTS {database}.{table_name}_histunion (
  -- All columns from inc table in exact order
  ...
);

-- INSERT with UNION ALL
INSERT INTO {database}.{table_name}_histunion
-- Historical data (with NULL for missing columns if needed)
SELECT ...
FROM {database}.{table_name}_hist
WHERE time > COALESCE((SELECT MAX(inc_value) FROM {lkup_db}.inc_log ...), 0)

UNION ALL

-- Incremental data
SELECT ...
FROM {database}.{table_name}
WHERE time > COALESCE((SELECT MAX(inc_value) FROM {lkup_db}.inc_log ...), 0);

-- Update watermarks
INSERT INTO {lkup_db}.inc_log ...
```

**Workflow File Structure:**
```yaml
timezone: UTC

_export:
  td:
    database: {database}
  lkup_db: {lkup_db}

+create_inc_log_table:
  td>:
  query: |
    CREATE TABLE IF NOT EXISTS ${lkup_db}.inc_log (...)

+hist_union_tasks:
  _parallel: true
  +{table_name}_histunion:
    td>: queries/{table_name}.sql
```

---

## Special Cases

### Full Load Tables
For `klaviyo_lists` and `klaviyo_metric_data`:
```sql
-- DROP TABLE (fresh start each run)
DROP TABLE IF EXISTS {database}.{table_name}_histunion;

-- CREATE TABLE (no IF NOT EXISTS)
CREATE TABLE {database}.{table_name}_histunion (...);

-- INSERT with NO WHERE clause (load all data)
INSERT INTO {database}.{table_name}_histunion
SELECT ... FROM {database}.{table_name}_hist
UNION ALL
SELECT ... FROM {database}.{table_name};

-- Still update watermarks (for tracking)
INSERT INTO {lkup_db}.inc_log ...
```

### Schema Differences
When inc table has columns that hist table doesn't:
```sql
-- CREATE uses inc schema (includes all columns)
CREATE TABLE IF NOT EXISTS {database}.{table_name}_histunion (
  incremental_date varchar,  -- Extra column from inc
  ...other columns...
);

-- Hist SELECT adds NULL for missing columns
SELECT
  NULL as incremental_date,  -- NULL for missing column
  ...other columns...
FROM {database}.{table_name}_hist

UNION ALL

-- Inc SELECT uses all columns
SELECT
  incremental_date,  -- Actual value
  ...other columns...
FROM {database}.{table_name}
```

---

## Next Steps After Generation

1. **Review Generated Files**:
   ```bash
   cat hist_union/queries/{table_name}.sql
   cat hist_union/hist_union_runner.dig
   ```

2. **Verify SQL Syntax**:
   ```bash
   cd hist_union
   td wf check hist_union_runner.dig
   ```

3. **Run Workflow**:
   ```bash
   td wf run hist_union_runner.dig
   ```

4. **Verify Results**:
   ```sql
   -- Check row counts
   SELECT COUNT(*) FROM {database}.{table_name}_histunion;

   -- Check watermarks
   SELECT * FROM {lkup_db}.inc_log
   WHERE project_name = 'hist_union'
   ORDER BY table_name;

   -- Sample data
   SELECT * FROM {database}.{table_name}_histunion
   LIMIT 10;
   ```

---

## Examples

### Example 1: Simple Table Name
```
User: "Create hist-union for mck_src.shopify_products"

I will derive:
- Inc: mck_src.shopify_products
- Hist: mck_src.shopify_products_hist
- Target: mck_src.shopify_products_histunion
- Lookup DB: mck_references (default)
```

### Example 2: Hist Table Name
```
User: "Add mck_src.klaviyo_events_hist to hist_union"

I will derive:
- Inc: mck_src.klaviyo_events
- Hist: mck_src.klaviyo_events_hist
- Target: mck_src.klaviyo_events_histunion
- Lookup DB: mck_references (default)
```

### Example 3: Custom Lookup DB
```
User: "Create hist-union for mc_src.users with lookup db mc_config"

I will use:
- Inc: mc_src.users
- Hist: mc_src.users_hist
- Target: mc_src.users_histunion
- Lookup DB: mc_config (user-specified)
```

---

## Production-Ready Guarantee

All generated code will:
- ✅ Use exact schemas from MCP tool (no guessing)
- ✅ Handle schema differences correctly
- ✅ Use correct template based on full load check
- ✅ Maintain exact column order
- ✅ Include proper NULL handling
- ✅ Update watermarks correctly
- ✅ Use parallel execution for efficiency
- ✅ Follow Presto/Trino SQL syntax
- ✅ Be production-tested and proven

---

**Ready to proceed? Please provide the table name(s) and I'll generate your complete hist-union workflow using exact schemas from MCP tool and production-tested templates.**
