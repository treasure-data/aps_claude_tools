---
name: hybrid-execute-snowflake
description: Execute Snowflake ID unification workflow with convergence detection and monitoring
---

# Execute Snowflake ID Unification Workflow

## Overview

Execute your generated Snowflake SQL workflow with intelligent convergence detection, real-time monitoring, and interactive error handling. This command orchestrates the complete unification process from graph creation to master table generation.

---

## What You Need

### Required Inputs
1. **SQL Directory**: Path to generated SQL files (e.g., `snowflake_sql/unify/`)
2. **Account**: Snowflake account name (e.g., `myaccount` from `myaccount.snowflakecomputing.com`)
3. **User**: Snowflake username
4. **Database**: Target database name
5. **Schema**: Target schema name
6. **Warehouse**: Compute warehouse name (defaults to `COMPUTE_WH`)

### Authentication
**Option 1: Password**
- Can be provided as argument or via environment variable `SNOWFLAKE_PASSWORD`
- Will prompt if not provided

**Option 2: SSO (externalbrowser)**
- Opens browser for authentication
- No password required

**Option 3: Key-Pair**
- Private key path via `SNOWFLAKE_PRIVATE_KEY_PATH`
- Passphrase via `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`

---

## What I'll Do

### Step 1: Connection Setup
- Connect to your Snowflake account
- Validate credentials and permissions
- Set database and schema context
- Verify SQL directory exists
- Activate warehouse

### Step 2: Execution Plan
Display execution plan with:
- All SQL files in execution order
- File types (Setup, Loop Iteration, Enrichment, Master Table, etc.)
- Estimated steps and dependencies

### Step 3: SQL Execution
I'll call the **snowflake-workflow-executor agent** to:
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
  → Continue to iteration 2

Iteration 2:
  ✓ Execute unify SQL
  • Check convergence: 450 records updated
  → Continue to iteration 3

Iteration 3:
  ✓ Execute unify SQL
  • Check convergence: 0 records updated
  ✓ CONVERGED! Stop loop
```

**Features**:
- Runs until convergence (updated_count = 0)
- Maximum 30 iterations safety limit
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
/cdp-hybrid-idu:hybrid-execute-snowflake

I'll prompt you for:
- SQL directory path
- Snowflake account name
- Username
- Database and schema
- Warehouse name
- Authentication method
```

### Advanced Mode
Provide all parameters upfront:
```
SQL directory: snowflake_sql/unify/
Account: myaccount
User: myuser
Database: my_database
Schema: my_schema
Warehouse: COMPUTE_WH
Password: (will prompt if not in environment)
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

### 2. Interactive Error Handling
If an error occurs:
```
✗ File: 04_unify_loop_iteration_01.sql
Error: Table not found: source_table

Continue with remaining files? (y/n):
```

You can choose to:
- Continue: Skip failed file, continue with rest
- Stop: Halt execution for investigation

### 3. Real-Time Monitoring
Track progress with:
- ✓ Completed steps (green)
- • Progress indicators (cyan)
- ✗ Failed steps (red)
- ⚠ Warnings (yellow)
- Row counts and execution times

### 4. Alias Table Creation
After convergence, creates:
```sql
CREATE OR REPLACE TABLE database.schema.unified_id_graph_unify_loop_final
AS SELECT * FROM database.schema.unified_id_graph_unify_loop_3
```

This allows downstream SQL to reference `loop_final` regardless of actual iteration count.

---

## Technical Details

### Python Script Execution
The agent executes:
```bash
python3 scripts/snowflake/snowflake_sql_executor.py \
    snowflake_sql/unify/ \
    --account myaccount \
    --user myuser \
    --database my_database \
    --schema my_schema \
    --warehouse COMPUTE_WH
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
SQL directory: snowflake_sql/unify/
Account: myorg-myaccount
User: analytics_user
Database: customer_data
Schema: id_unification
Warehouse: LARGE_WH
```

### Output
```
✓ Connected to Snowflake: myorg-myaccount
• Using database: customer_data, schema: id_unification

Starting Snowflake SQL Execution
• Database: customer_data
• Schema: id_unification

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

--- Iteration 2 ---
✓ Iteration 2 completed
• Rows processed: 125000
• Updated records: 450

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

• Disconnected from Snowflake
```

---

## Monitoring and Troubleshooting

### Check Execution Progress
During execution, you can monitor:
- Snowflake query history
- Table sizes and row counts
- Warehouse utilization
- Execution logs

### Common Issues

**Issue**: Connection timeout
**Solution**: Check network access, verify credentials, ensure warehouse is running

**Issue**: Table not found
**Solution**: Verify database/schema permissions, check source table names in YAML

**Issue**: Loop doesn't converge
**Solution**: Check data quality, increase max_iterations, review key validation rules

**Issue**: Warehouse suspended
**Solution**: Ensure auto-resume is enabled, manually resume warehouse if needed

**Issue**: Permission denied
**Solution**: Verify database/schema permissions, check role assignments

### Performance Optimization
- Use larger warehouse for faster execution (L, XL, 2XL, etc.)
- Enable multi-cluster warehouse for concurrency
- Use clustering keys on frequently joined columns
- Monitor query profiles for optimization opportunities

---

## Post-Execution Validation

### Check Coverage
```sql
SELECT
    COUNT(*) as total_records,
    COUNT(unified_id) as records_with_id,
    COUNT(unified_id) * 100.0 / COUNT(*) as coverage_percent
FROM database.schema.enriched_customer_profiles;
```

### Verify Master Table
```sql
SELECT COUNT(*) as unified_customers
FROM database.schema.customer_master;
```

### Review Statistics
```sql
SELECT * FROM database.schema.unified_id_result_key_stats
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

## Authentication Examples

### Using Password
```bash
export SNOWFLAKE_PASSWORD='your_password'
/cdp-hybrid-idu:hybrid-execute-snowflake
```

### Using SSO
```bash
/cdp-hybrid-idu:hybrid-execute-snowflake
# Will prompt: Use SSO authentication? (y/n): y
# Opens browser for authentication
```

### Using Key-Pair
```bash
export SNOWFLAKE_PRIVATE_KEY_PATH='/path/to/key.p8'
export SNOWFLAKE_PRIVATE_KEY_PASSPHRASE='passphrase'
/cdp-hybrid-idu:hybrid-execute-snowflake
```

---

**Ready to execute your Snowflake ID unification workflow?**

Provide your SQL directory path and Snowflake connection details to begin!
