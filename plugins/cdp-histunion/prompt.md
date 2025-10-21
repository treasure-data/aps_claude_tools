# CDP Hist-Union Plugin - Production-Ready Workflow Generator

## Overview
This plugin creates production-ready hist-union workflows to combine historical and incremental table data in Treasure Data. It follows the exact specifications from the working CLAUDE.md prompt.

## ⚠️ CRITICAL: THREE GOLDEN RULES - ENFORCE AT ALL TIMES ⚠️

### 1️⃣ ALWAYS USE MCP TOOL TO GET TABLE SCHEMA - NO GUESSING
**YOU MUST use `mcp__mcc_treasuredata__describe_table` for BOTH inc and hist tables:**
- Get exact column list from actual table schema
- **CRITICAL**: Some incremental tables have extra columns (like `incremental_date`) that historical tables DON'T have
- **ALWAYS** use the INCREMENTAL table schema as BASE for histunion table creation
- Compare column structures between inc and hist tables to identify differences
- Never assume or guess column names or data types

### 2️⃣ CHECK FULL LOAD LIST FIRST - DIFFERENT TEMPLATE
**YOU MUST check if table is in FULL LOAD list:**
- **FULL LOAD tables**: `klaviyo_lists_histunion`, `klaviyo_metric_data_histunion`
- **IF FULL LOAD**: Use Case 3 template (DROP TABLE, no WHERE clause)
- **IF INCREMENTAL**: Use Case 1 or Case 2 template (with WHERE clause and watermarks)
- Full load tables still update watermarks but ignore them in WHERE clauses

### 3️⃣ PARSE USER INPUT INTELLIGENTLY - EXACT TABLE NAMES
**YOU MUST intelligently derive inc, hist, and target table names:**
- Parse database and table name from user input
- If table name contains `_hist` or `_histunion` suffix, remove it to get base name
- Construct exact table names:
  - Inc: `{database}.{base_name}`
  - Hist: `{database}.{base_name}_hist`
  - Target: `{database}.{base_name}_histunion`

**Breaking these rules = Incorrect schema = Production failure**

---

## 🔒 STRICT WORKFLOW RULES

1. **NEVER IMPROVISE** - Follow exact SQL template structure
2. **ALWAYS GET SCHEMA FIRST** - Use MCP tool for both inc and hist tables
3. **ALWAYS COMPARE SCHEMAS** - Check for column differences between inc/hist
4. **ALWAYS USE INC SCHEMA AS BASE** - Histunion table follows incremental table schema
5. **ALWAYS CHECK COLUMN ORDER** - Maintain exact order in SELECT statements
6. **ALWAYS USE inc_log FOR WATERMARKS** - Never use MAX from target table
7. **ALWAYS UPDATE WATERMARKS** - Even for FULL LOAD tables
8. **ALWAYS USE time COLUMN** - For incremental filtering (not for FULL LOAD)
9. **ALWAYS CREATE inc_log TABLE** - First task in workflow
10. **ALWAYS USE PARALLEL EXECUTION** - Wrap hist_union tasks with `_parallel: true`

---

## Required Information from User

- **Incremental table name** (e.g., `mc_src.klaviyo_events`)
- **Historical table name** (e.g., `mc_src.klaviyo_events_hist`)
- **Target histunion table name** (e.g., `mc_src.klaviyo_events_histunion`)
- **Optional**: Lookup/config database name (defaults to `config_db` if not specified)

---

## Parse User Input Intelligently **CRITICAL**

Example input: "Please add client_src.shopify_products and client_src.shopify_product_variants_hist table in hist_union project"

1. **Parse database and table_name from input**:
   - database = `client_src`
   - table_name = `shopify_products` and `shopify_product_variants_hist`

2. **Remove _hist or _histunion suffix if present**:

   For table 1:
   - Inc: `client_src.shopify_products`
   - Hist: `client_src.shopify_products_hist`
   - Target: `client_src.shopify_products_histunion`

   For table 2:
   - Inc: `client_src.shopify_product_variants`
   - Hist: `client_src.shopify_product_variants_hist`
   - Target: `client_src.shopify_product_variants_histunion`

---

## Step-by-Step Process

### 1. Get Table Schema **CRITICAL**

**YOU MUST use MCP tool to get exact schema:**
```
Use mcp__mcc_treasuredata__describe_table for:
1. Incremental table (e.g., mc_src.klaviyo_events)
2. Historical table (e.g., mc_src.klaviyo_events_hist)
```

**CRITICAL Schema Comparison:**
- Some incremental tables have extra columns that historical tables DON'T have (e.g., `incremental_date`)
- **ALWAYS** use INCREMENTAL table schema as BASE for histunion table
- Compare schemas to identify column differences
- Never assume `incremental_date` exists - check actual schema
- Always use exact column names from schema

### 2. Create SQL File

**Directory Structure:**
```
mkdir -p hist_union/queries
```

**File Naming:**
- Create SQL file named after incremental table
- Example: `hist_union/queries/klaviyo_events.sql`

**CRITICAL SQL Rules:**
- Use exact column order from table schema
- Include ALL columns from tables in exact same order
- For UNION ALL, both SELECT statements must have identical column structure

### SQL Template Structure:

**Case 1: When incremental table has SAME columns as historical table:**
```sql
-- Create histunion table if not exists
CREATE TABLE IF NOT EXISTS {database}.{table_name}_histunion (
  {all_columns_from_incremental_table_schema_exact_match}
);

-- Insert incremental data from both hist and inc tables
INSERT INTO {database}.{table_name}_histunion
-- Historical data
SELECT
  {all_column_names_in_exact_order}
FROM {database}.{table_name}_hist
WHERE time > COALESCE(
  (SELECT MAX(inc_value) FROM {lkup_db}.inc_log
   WHERE table_name = '{table_name}_hist' AND project_name = 'hist_union'),
  0
)

UNION ALL

-- Incremental data
SELECT
  {all_column_names_in_exact_order}
FROM {database}.{table_name}
WHERE time > COALESCE(
  (SELECT MAX(inc_value) FROM {lkup_db}.inc_log
   WHERE table_name = '{table_name}' AND project_name = 'hist_union'),
  0
);

-- Update watermark for historical table
INSERT INTO {lkup_db}.inc_log
SELECT '{table_name}_hist' table_name, 'hist_union' project_name, MAX(time) inc_value
FROM {database}.{table_name}_hist;

-- Update watermark for incremental table
INSERT INTO {lkup_db}.inc_log
SELECT '{table_name}' table_name, 'hist_union' project_name, MAX(time) inc_value
FROM {database}.{table_name};
```

**Case 2: When incremental table has EXTRA column (e.g., incremental_date) that historical table lacks:**
```sql
-- Create histunion table if not exists
-- **CRITICAL**: Use INCREMENTAL table schema as base (includes incremental_date column)
CREATE TABLE IF NOT EXISTS {database}.{table_name}_histunion (
  incremental_date varchar,  -- Extra column from incremental table
  {remaining_columns_from_incremental_table_schema}
);

-- Insert incremental data from both hist and inc tables
INSERT INTO {database}.{table_name}_histunion
-- Historical data (add NULL for incremental_date since hist table doesn't have it)
SELECT
  NULL as incremental_date,  -- Add NULL for missing incremental_date column
  {remaining_column_names_in_exact_order}
FROM {database}.{table_name}_hist
WHERE time > COALESCE(
  (SELECT MAX(inc_value) FROM {lkup_db}.inc_log
   WHERE table_name = '{table_name}_hist' AND project_name = 'hist_union'),
  0
)

UNION ALL

-- Incremental data (use all columns including incremental_date)
SELECT
  incremental_date,  -- Actual value from incremental table
  {remaining_column_names_in_exact_order}
FROM {database}.{table_name}
WHERE time > COALESCE(
  (SELECT MAX(inc_value) FROM {lkup_db}.inc_log
   WHERE table_name = '{table_name}' AND project_name = 'hist_union'),
  0
);

-- Update watermark for historical table
INSERT INTO {lkup_db}.inc_log
SELECT '{table_name}_hist' table_name, 'hist_union' project_name, MAX(time) inc_value
FROM {database}.{table_name}_hist;

-- Update watermark for incremental table
INSERT INTO {lkup_db}.inc_log
SELECT '{table_name}' table_name, 'hist_union' project_name, MAX(time) inc_value
FROM {database}.{table_name};
```

**Case 3: FULL LOAD (for klaviyo_lists and klaviyo_metric_data ONLY):**
```sql
-- Drop and recreate histunion table for full load
DROP TABLE IF EXISTS {database}.{table_name}_histunion;

CREATE TABLE {database}.{table_name}_histunion (
  {all_columns_from_incremental_table_schema_exact_match}
);

-- Full load from both hist and inc tables (NO WHERE CLAUSE)
INSERT INTO {database}.{table_name}_histunion
-- Historical data
SELECT
  {all_column_names_in_exact_order}
FROM {database}.{table_name}_hist

UNION ALL

-- Incremental data
SELECT
  {all_column_names_in_exact_order}
FROM {database}.{table_name};

-- Update watermark for historical table
INSERT INTO {lkup_db}.inc_log
SELECT '{table_name}_hist' table_name, 'hist_union' project_name, MAX(time) inc_value
FROM {database}.{table_name}_hist;

-- Update watermark for incremental table
INSERT INTO {lkup_db}.inc_log
SELECT '{table_name}' table_name, 'hist_union' project_name, MAX(time) inc_value
FROM {database}.{table_name};
```

### 3. Create Digdag Workflow File

**File Location:** `hist_union/hist_union_runner.dig`

**Template:**
```yaml
timezone: UTC

_export:
  td:
    database: {database_name}
  lkup_db: {lkup_db_name}

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

  +{incremental_table_name}_histunion:
    td>: queries/{incremental_table_name}.sql
```

**CRITICAL**: Remove any schedule block from the workflow file.

---

## Critical Rules

1. **NEVER** use pattern matching for table names - use EXACT names from user
2. **CRITICAL**: **CHECK FIRST** if table is in FULL LOAD list
   - **IF FULL LOAD**: Use Case 3 template (DROP TABLE, no WHERE clause)
   - **IF INCREMENTAL**: Use Case 1 or Case 2 template (with WHERE clause)
3. **CRITICAL**: **ALWAYS** use INCREMENTAL table schema as BASE for histunion table
4. **CRITICAL**: **ALWAYS** use MCP tool to get exact schemas - NO GUESSING
5. **CRITICAL**: Handle column differences between incremental and historical tables
   - **COMPARE** schemas first using MCP tool results
   - **IF** incremental has extra columns: Add `NULL as column_name` for hist SELECTs
   - **IF** schemas identical: Use same column structure for both SELECTs
   - **NEVER** assume `incremental_date` exists without checking
6. **ALWAYS** maintain exact column order in UNION ALL selects
7. **ALWAYS** include ALL columns from both tables
8. **ALWAYS** use inc_log table for watermarking
9. **ALWAYS** use `time` column for incremental logic (except FULL LOAD)
10. **ALWAYS** create SQL file named after incremental table
11. **ALWAYS** verify table schemas using MCP tool before creating SQL
12. **ALWAYS** create inc_log table first in workflow
13. **ALWAYS** update watermarks for both hist and inc tables (even FULL LOAD)
14. **ALWAYS** use project_name = 'hist_union' in inc_log entries
15. **ALWAYS** wrap hist_union tasks with `_parallel: true` for concurrent execution
16. **CRITICAL**: **ALWAYS** use `config_db` as default lookup database (lkup_db) unless user specifies different
17. **MANDATORY**: Auto-set lkup_db to `config_db` if user doesn't specify
18. **SQL SYNTAX**: For Presto/Trino compatibility:
    - Use double quotes `"column_name"` for reserved keywords (like "index")
    - NEVER use backticks `` `column_name` `` (not supported)

---

## Full Load Tables

**These tables ALWAYS use FULL LOAD (Case 3 template):**
- `client_src.klaviyo_lists_histunion`
- `client_src.klaviyo_metric_data_histunion`

**FULL LOAD means:**
- DROP and recreate histunion table every run
- Load ALL data from both hist and inc tables (NO WHERE clause)
- Still update watermarks for tracking purposes

---

## File Structure

```
project_root/
├── hist_union/
    ├── hist_union_runner.dig
    └── queries/
        └── {incremental_table_name}.sql
```

---

## Example

**For tables:**
- Inc: `mc_src.klaviyo_events`
- Hist: `mc_src.klaviyo_events_hist`
- Target: `mc_src.klaviyo_events_histunion`

**Creates:**
- `hist_union/hist_union_runner.dig`
- `hist_union/queries/klaviyo_events.sql`

---

## ✅ REQUIRED VERIFICATION BEFORE DELIVERING CODE

**Before presenting ANY generated file, verify:**

1. ✅ **Schema verified**: Used MCP tool for both inc and hist tables
2. ✅ **Columns compared**: Identified any differences between inc/hist schemas
3. ✅ **Correct template**: Used Case 1, 2, or 3 based on schema comparison and full load check
4. ✅ **All columns present**: Every column from inc table schema included
5. ✅ **Exact column order**: Matches incremental table schema exactly
6. ✅ **Correct NULL handling**: Added NULL for columns missing in hist table (if applicable)
7. ✅ **Watermarks included**: Both hist and inc table watermark updates present
8. ✅ **Parallel execution**: _parallel: true wrapper present in workflow
9. ✅ **No schedule block**: Schedule removed from workflow
10. ✅ **Correct lkup_db**: Set to config_db or user-specified value

---

## 🚨 MANDATORY RESPONSE PATTERN

**When user requests hist-union workflow creation:**

```
1. Parse table names intelligently:
   - Extract database and base table names
   - Remove _hist or _histunion suffixes
   - Construct inc, hist, and target names

2. Get table schemas using MCP tool:
   - Call mcp__mcc_treasuredata__describe_table for inc table
   - Call mcp__mcc_treasuredata__describe_table for hist table
   - Compare column structures

3. Check full load status:
   - Determine if table is in FULL LOAD list
   - Select appropriate template (Case 1, 2, or 3)

4. Generate ALL files in ONE response:
   - Create SQL file with exact column order
   - Create/update Digdag workflow file
   - Include proper NULL handling if schemas differ

5. Verify and report:
   - Confirm all verification gates passed
   - List files created
   - Provide next steps
```

---

## Production-Ready Guarantee

**By following these instructions, you ensure:**

- ✅ Correct schema matching using MCP tool
- ✅ Proper handling of column differences
- ✅ Complete watermark tracking
- ✅ Efficient parallel processing
- ✅ Production-tested patterns
- ✅ No manual errors or assumptions

---

**YOU ARE NOW READY TO GENERATE PRODUCTION-READY HIST-UNION WORKFLOWS!**

Follow the THREE GOLDEN RULES at all times. Always use MCP tool for schemas. Check full load list first. Parse user input intelligently. No exceptions.
