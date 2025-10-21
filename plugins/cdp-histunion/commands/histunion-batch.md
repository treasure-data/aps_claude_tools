---
name: histunion-batch
description: Create hist-union workflows for multiple tables in batch with parallel processing
---

# Create Batch Hist-Union Workflows

## ⚠️ CRITICAL: This command processes multiple tables efficiently with schema validation

I'll help you create hist-union workflows for multiple tables at once, with proper schema validation for each table.

---

## Required Information

### 1. Table List
Provide table names in any format (comma-separated or one per line):

**Option A - Base names:**
```
client_src.klaviyo_events, client_src.shopify_products, client_src.onetrust_profiles
```

**Option B - Hist names:**
```
client_src.klaviyo_events_hist
client_src.shopify_products_hist
client_src.onetrust_profiles_hist
```

**Option C - Mixed formats:**
```
client_src.klaviyo_events, client_src.shopify_products_hist, client_src.onetrust_profiles
```

**Option D - List format:**
```
- client_src.klaviyo_events
- client_src.shopify_products
- client_src.onetrust_profiles
```

### 2. Lookup Database (Optional)
- **Lookup/Config Database**: Database for inc_log watermark table
- **Default**: `client_config` (will be used if not specified)

---

## What I'll Do

### Step 1: Parse All Table Names
I will parse and normalize all table names:
```
For each table in list:
1. Extract database and base name
2. Remove _hist or _histunion suffix if present
3. Derive:
   - Inc table: {database}.{base_name}
   - Hist table: {database}.{base_name}_hist
   - Target table: {database}.{base_name}_histunion
```

### Step 2: Get Schemas for All Tables via MCP Tool
**CRITICAL**: I will get exact schemas for EVERY table:
```
For each table:
1. Call mcp__mcc_treasuredata__describe_table for inc table
   - Get complete column list
   - Get exact column order
   - Get data types

2. Call mcp__mcc_treasuredata__describe_table for hist table
   - Get complete column list
   - Get exact column order
   - Get data types

3. Compare schemas:
   - Document column differences
   - Note any extra columns in inc vs hist
   - Record exact column order
```

**Note**: This may require multiple MCP calls. I'll process them efficiently.

### Step 3: Check Full Load Status for Each Table
I will check each table against full load list:
```
For each table:
IF table_name IN ('klaviyo_lists', 'klaviyo_metric_data'):
    template[table] = 'FULL_LOAD'  # Case 3
ELSE:
    IF inc_schema == hist_schema:
        template[table] = 'IDENTICAL'  # Case 1
    ELSE:
        template[table] = 'EXTRA_COLUMNS'  # Case 2
```

### Step 4: Generate SQL Files for All Tables
I will create SQL file for each table in ONE response:
```
For each table, create: hist_union/queries/{base_name}.sql

With correct template based on schema analysis:
- Case 1: Identical schemas
- Case 2: Inc has extra columns
- Case 3: Full load

All files created in parallel using multiple Write tool calls
```

### Step 5: Update Digdag Workflow
I will update workflow with all tables:
```
File: hist_union/hist_union_runner.dig

Structure:
+hist_union_tasks:
  _parallel: true

  +{table1_name}_histunion:
    td>: queries/{table1_name}.sql

  +{table2_name}_histunion:
    td>: queries/{table2_name}.sql

  +{table3_name}_histunion:
    td>: queries/{table3_name}.sql

  ... (all tables)
```

### Step 6: Verify Quality Gates for All Tables
Before delivering, I will verify for EACH table:
```
For each table:
✅ MCP tool used for both inc and hist schemas
✅ Schema differences identified
✅ Correct template selected
✅ All inc columns present in exact order
✅ NULL handling correct for missing columns
✅ Watermarks included for both hist and inc
✅ Parallel execution configured
```

---

## Batch Processing Strategy

### Efficient MCP Usage
```
1. Collect all table names first
2. Make MCP calls for all inc tables
3. Make MCP calls for all hist tables
4. Compare all schemas in batch
5. Generate all SQL files in ONE response
6. Update workflow once with all tasks
```

### Parallel File Generation
I will use multiple Write tool calls in a SINGLE response:
```
Single Response Contains:
- Write: hist_union/queries/table1.sql
- Write: hist_union/queries/table2.sql
- Write: hist_union/queries/table3.sql
- ... (all tables)
- Edit: hist_union/hist_union_runner.dig (add all tasks)
```

---

## Output

I will generate:

### For N Tables:
1. **hist_union/queries/{table1}.sql** - SQL for table 1
2. **hist_union/queries/{table2}.sql** - SQL for table 2
3. **hist_union/queries/{table3}.sql** - SQL for table 3
4. ... (one SQL file per table)
5. **hist_union/hist_union_runner.dig** - Updated workflow with all tables

### Workflow Structure:
```yaml
timezone: UTC

_export:
  td:
    database: {database}
  lkup_db: {lkup_db}

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

  +table1_histunion:
    td>: queries/table1.sql

  +table2_histunion:
    td>: queries/table2.sql

  +table3_histunion:
    td>: queries/table3.sql

  # ... all tables processed in parallel
```

---

## Progress Reporting

During processing, I will report:

### Phase 1: Parsing
```
Parsing table names...
✅ Found 5 tables to process:
  1. client_src.klaviyo_events
  2. client_src.shopify_products
  3. client_src.onetrust_profiles
  4. client_src.klaviyo_lists (FULL LOAD)
  5. client_src.users
```

### Phase 2: Schema Retrieval
```
Retrieving schemas via MCP tool...
✅ Got schema for client_src.klaviyo_events (inc)
✅ Got schema for client_src.klaviyo_events_hist (hist)
✅ Got schema for client_src.shopify_products (inc)
✅ Got schema for client_src.shopify_products_hist (hist)
... (all tables)
```

### Phase 3: Schema Analysis
```
Analyzing schemas...
✅ Table 1: Identical schemas - Use Case 1
✅ Table 2: Inc has extra 'incremental_date' - Use Case 2
✅ Table 3: Identical schemas - Use Case 1
✅ Table 4: FULL LOAD - Use Case 3
✅ Table 5: Identical schemas - Use Case 1
```

### Phase 4: File Generation
```
Generating all files...
✅ Created hist_union/queries/klaviyo_events.sql
✅ Created hist_union/queries/shopify_products.sql
✅ Created hist_union/queries/onetrust_profiles.sql
✅ Created hist_union/queries/klaviyo_lists.sql (FULL LOAD)
✅ Created hist_union/queries/users.sql
✅ Updated hist_union/hist_union_runner.dig with 5 parallel tasks
```

---

## Special Handling

### Mixed Databases
If tables are from different databases:
```
✅ Supported - Each SQL file uses correct database
✅ Workflow uses primary database in _export
✅ Individual tasks can override if needed
```

### Full Load Tables in Batch
```
✅ Automatically detected (klaviyo_lists, klaviyo_metric_data)
✅ Uses Case 3 template (DROP + CREATE, no WHERE)
✅ Still updates watermarks
✅ Processed in parallel with other tables
```

### Schema Differences
```
✅ Each table analyzed independently
✅ NULL handling applied only where needed
✅ Exact column order maintained per table
✅ Template selection per table based on schema
```

---

## Performance Benefits

### Why Batch Processing?
- ✅ **Faster**: All files created in one response
- ✅ **Consistent**: Single workflow file with all tasks
- ✅ **Efficient**: Parallel MCP calls where possible
- ✅ **Complete**: All tables configured together
- ✅ **Parallel Execution**: All tasks run concurrently in Treasure Data

### Execution Efficiency
```
Sequential Processing:
Table 1: 10 min
Table 2: 10 min
Table 3: 10 min
Total: 30 minutes

Parallel Processing:
All tables: ~10 minutes (depending on slowest table)
```

---

## Next Steps After Generation

1. **Review All Generated Files**:
   ```bash
   ls -la hist_union/queries/
   cat hist_union/hist_union_runner.dig
   ```

2. **Verify Workflow Syntax**:
   ```bash
   cd hist_union
   td wf check hist_union_runner.dig
   ```

3. **Run Batch Workflow**:
   ```bash
   td wf run hist_union_runner.dig
   ```

4. **Monitor Progress**:
   ```bash
   td wf logs hist_union_runner.dig
   ```

5. **Verify All Results**:
   ```sql
   -- Check watermarks for all tables
   SELECT * FROM {lkup_db}.inc_log
   WHERE project_name = 'hist_union'
   ORDER BY table_name;

   -- Check row counts for all histunion tables
   SELECT
     '{table1}_histunion' as table_name,
     COUNT(*) as row_count
   FROM {database}.{table1}_histunion
   UNION ALL
   SELECT
     '{table2}_histunion',
     COUNT(*)
   FROM {database}.{table2}_histunion
   -- ... (for all tables)
   ```

---

## Example

### Input
```
Create hist-union for these tables:
- client_src.klaviyo_events
- client_src.shopify_products_hist
- client_src.onetrust_profiles
- client_src.klaviyo_lists
```

### Output Summary
```
✅ Processed 4 tables:

1. klaviyo_events (Incremental - Case 1: Identical schemas)
   - Inc: client_src.klaviyo_events
   - Hist: client_src.klaviyo_events_hist
   - Target: client_src.klaviyo_events_histunion

2. shopify_products (Incremental - Case 2: Inc has extra columns)
   - Inc: client_src.shopify_products
   - Hist: client_src.shopify_products_hist
   - Target: client_src.shopify_products_histunion
   - Extra columns in inc: incremental_date

3. onetrust_profiles (Incremental - Case 1: Identical schemas)
   - Inc: client_src.onetrust_profiles
   - Hist: client_src.onetrust_profiles_hist
   - Target: client_src.onetrust_profiles_histunion

4. klaviyo_lists (FULL LOAD - Case 3)
   - Inc: client_src.klaviyo_lists
   - Hist: client_src.klaviyo_lists_hist
   - Target: client_src.klaviyo_lists_histunion

Created 4 SQL files + 1 workflow file
All tasks configured for parallel execution
```

---

## Production-Ready Guarantee

All generated code will:
- ✅ Use exact schemas from MCP tool for every table
- ✅ Handle schema differences correctly per table
- ✅ Use correct template based on individual table analysis
- ✅ Process all tables in parallel for maximum efficiency
- ✅ Maintain exact column order per table
- ✅ Include proper NULL handling where needed
- ✅ Update watermarks for all tables
- ✅ Follow Presto/Trino SQL syntax
- ✅ Be production-tested and proven

---

**Ready to proceed? Please provide your list of tables and I'll generate complete hist-union workflows for all of them using exact schemas from MCP tool and production-tested templates.**
