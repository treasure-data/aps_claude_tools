---
name: hybrid-execute-databricks
description: Execute Databricks ID unification workflow with convergence detection and monitoring
---

# Execute Databricks ID Unification Workflow

## Overview

Execute your generated Databricks SQL workflow with intelligent convergence detection, real-time monitoring, and interactive error handling. This command orchestrates the complete unification process from graph creation to master table generation.

---

## What You Need

### Required Inputs
1. **SQL Directory**: Path to generated SQL files (e.g., `databricks_sql/unify/`)
2. **Server Hostname**: Your Databricks workspace URL (e.g., `your-workspace.cloud.databricks.com`)
3. **HTTP Path**: SQL Warehouse or cluster path (e.g., `/sql/1.0/warehouses/abc123`)
4. **Catalog**: Target catalog name
5. **Schema**: Target schema name

### Authentication
**Option 1: Personal Access Token (PAT)**
- Access token from Databricks workspace
- Can be provided as argument or via environment variable `DATABRICKS_TOKEN`

**Option 2: OAuth**
- Browser-based authentication
- No token required, will open browser for login

---

## What I'll Do

### Step 1: Connection Setup
- Connect to your Databricks workspace
- Validate credentials and permissions
- Set catalog and schema context
- Verify SQL directory exists

### Step 2: Execution Plan
Display execution plan with:
- All SQL files in execution order
- File types (Setup, Loop Iteration, Enrichment, Master Table, etc.)
- Estimated steps and dependencies

### Step 3: SQL Execution
I'll call the **databricks-workflow-executor agent** to:
- Execute SQL files in proper sequence
- Skip loop iteration files (handled separately)
- Monitor progress with real-time feedback
- Track row counts and execution times

### Step 4: Unify Loop with Convergence Detection
**Intelligent Loop Execution**:
```
Iteration 1:
  ✓ Execute unify SQL
  • Check convergence: 1500 records updated
  • Optimize Delta table
  → Continue to iteration 2

Iteration 2:
  ✓ Execute unify SQL
  • Check convergence: 450 records updated
  • Optimize Delta table
  → Continue to iteration 3

Iteration 3:
  ✓ Execute unify SQL
  • Check convergence: 0 records updated
  ✓ CONVERGED! Stop loop
```

**Features**:
- Runs until convergence (updated_count = 0)
- Maximum 30 iterations safety limit
- Auto-optimization after each iteration
- Creates alias table (loop_final) for downstream processing

### Step 5: Post-Loop Processing
- Execute canonicalization step
- Generate result statistics
- Enrich source tables with canonical IDs
- Create master tables
- Generate metadata and lookup tables

### Step 6: Final Report
Provide:
- Total execution time
- Files processed successfully
- Convergence statistics
- Final table row counts
- Next steps and recommendations

---

## Command Usage

### Interactive Mode (Recommended)
```
/cdp-hybrid-idu:hybrid-execute-databricks

I'll prompt you for:
- SQL directory path
- Databricks server hostname
- HTTP path
- Catalog and schema
- Authentication method
```

### Advanced Mode
Provide all parameters upfront:
```
SQL directory: databricks_sql/unify/
Server hostname: your-workspace.cloud.databricks.com
HTTP path: /sql/1.0/warehouses/abc123
Catalog: my_catalog
Schema: my_schema
Auth type: pat (or oauth)
Access token: dapi... (if using PAT)
```

---

## Execution Features

### 1. Convergence Detection
**Algorithm**:
```sql
SELECT COUNT(*) as updated_count FROM (
    SELECT leader_ns, leader_id, follower_ns, follower_id
    FROM current_iteration
    EXCEPT
    SELECT leader_ns, leader_id, follower_ns, follower_id
    FROM previous_iteration
) diff
```

**Stops when**: updated_count = 0

### 2. Delta Table Optimization
After major operations:
```sql
OPTIMIZE table_name
```
Benefits:
- Compacts small files
- Improves query performance
- Reduces storage costs
- Optimizes clustering

### 3. Interactive Error Handling
If an error occurs:
```
✗ File: 04_unify_loop_iteration_01.sql
Error: Table not found: source_table

Continue with remaining files? (y/n):
```

You can choose to:
- Continue: Skip failed file, continue with rest
- Stop: Halt execution for investigation

### 4. Real-Time Monitoring
Track progress with:
- ✓ Completed steps (green)
- • Progress indicators (cyan)
- ✗ Failed steps (red)
- ⚠ Warnings (yellow)
- Row counts and execution times

### 5. Alias Table Creation
After convergence, creates:
```sql
CREATE OR REPLACE TABLE catalog.schema.unified_id_graph_unify_loop_final
AS SELECT * FROM catalog.schema.unified_id_graph_unify_loop_3
```

This allows downstream SQL to reference `loop_final` regardless of actual iteration count.

---

## Technical Details

### Python Script Execution
The agent executes:
```bash
python3 scripts/databricks/databricks_sql_executor.py \
    databricks_sql/unify/ \
    --server-hostname your-workspace.databricks.com \
    --http-path /sql/1.0/warehouses/abc123 \
    --catalog my_catalog \
    --schema my_schema \
    --auth-type pat \
    --optimize-tables
```

### Execution Order
1. **Setup Phase** (01-03):
   - Create graph table (loop_0)
   - Extract and merge identities
   - Generate source statistics

2. **Unification Loop** (04):
   - Run iterations until convergence
   - Check after EVERY iteration
   - Stop when updated_count = 0
   - Create loop_final alias

3. **Canonicalization** (05):
   - Create canonical ID lookup
   - Create keys and tables metadata
   - Rename final graph table

4. **Statistics** (06):
   - Generate result key statistics
   - Create histograms
   - Calculate coverage metrics

5. **Enrichment** (10-19):
   - Add canonical IDs to source tables
   - Create enriched_* tables

6. **Master Tables** (20-29):
   - Aggregate attributes
   - Apply priority rules
   - Create unified customer profiles

7. **Metadata** (30-39):
   - Unification metadata
   - Filter lookup tables
   - Column lookup tables

### Connection Management
- Establishes single connection for entire workflow
- Uses connection pooling for efficiency
- Automatic reconnection on timeout
- Proper cleanup on completion or error

---

## Example Execution

### Input
```
SQL directory: databricks_sql/unify/
Server hostname: dbc-12345-abc.cloud.databricks.com
HTTP path: /sql/1.0/warehouses/6789abcd
Catalog: customer_data
Schema: id_unification
Auth type: pat
```

### Output
```
✓ Connected to Databricks: dbc-12345-abc.cloud.databricks.com
• Using catalog: customer_data, schema: id_unification

Starting Databricks SQL Execution
• Catalog: customer_data
• Schema: id_unification
• Delta tables: ✓ enabled

Executing: 01_create_graph.sql
✓ 01_create_graph.sql: Executed successfully

Executing: 02_extract_merge.sql
✓ 02_extract_merge.sql: Executed successfully
• Rows affected: 125000

Executing: 03_source_key_stats.sql
✓ 03_source_key_stats.sql: Executed successfully

Executing Unify Loop Before Canonicalization

--- Iteration 1 ---
✓ Iteration 1 completed
• Rows processed: 125000
• Updated records: 1500
• Optimizing Delta table

--- Iteration 2 ---
✓ Iteration 2 completed
• Rows processed: 125000
• Updated records: 450
• Optimizing Delta table

--- Iteration 3 ---
✓ Iteration 3 completed
• Rows processed: 125000
• Updated records: 0
✓ Loop converged after 3 iterations

• Creating alias table for final iteration
✓ Alias table 'unified_id_graph_unify_loop_final' created

Executing: 05_canonicalize.sql
✓ 05_canonicalize.sql: Executed successfully

[... continues with enrichment, master tables, metadata ...]

Execution Complete
• Files processed: 18/18
• Final unified_id_lookup rows: 98,500

• Disconnected from Databricks
```

---

## Monitoring and Troubleshooting

### Check Execution Progress
During execution, you can monitor:
- Databricks SQL Warehouse query history
- Delta table sizes and row counts
- Execution logs in Databricks workspace

### Common Issues

**Issue**: Connection timeout
**Solution**: Check network access, verify credentials, ensure SQL Warehouse is running

**Issue**: Table not found
**Solution**: Verify catalog/schema permissions, check source table names in YAML

**Issue**: Loop doesn't converge
**Solution**: Check data quality, increase max_iterations, review key validation rules

**Issue**: Out of memory
**Solution**: Increase SQL Warehouse size, optimize clustering, reduce batch sizes

**Issue**: Permission denied
**Solution**: Verify catalog/schema permissions, check Unity Catalog access controls

### Performance Optimization
- Use larger SQL Warehouse for faster execution
- Enable auto-scaling for variable workloads
- Optimize Delta tables regularly
- Use clustering on frequently joined columns

---

## Post-Execution Validation

### Check Coverage
```sql
SELECT
    COUNT(*) as total_records,
    COUNT(unified_id) as records_with_id,
    COUNT(unified_id) * 100.0 / COUNT(*) as coverage_percent
FROM catalog.schema.enriched_customer_profiles;
```

### Verify Master Table
```sql
SELECT COUNT(*) as unified_customers
FROM catalog.schema.customer_master;
```

### Review Statistics
```sql
SELECT * FROM catalog.schema.unified_id_result_key_stats
WHERE from_table = '*';
```

---

## Success Criteria

Execution successful when:
- ✅ All SQL files processed without critical errors
- ✅ Unification loop converged (updated_count = 0)
- ✅ Canonical IDs generated for all eligible records
- ✅ Enriched tables created successfully
- ✅ Master tables populated with attributes
- ✅ Coverage metrics meet expectations

---

**Ready to execute your Databricks ID unification workflow?**

Provide your SQL directory path and Databricks connection details to begin!
