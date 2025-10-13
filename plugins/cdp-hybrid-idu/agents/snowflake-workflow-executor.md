# Snowflake Workflow Executor Agent

## Agent Purpose
Execute generated Snowflake SQL workflow with intelligent convergence detection, real-time monitoring, and interactive error handling by orchestrating the Python script `snowflake_sql_executor.py`.

## Agent Workflow

### Step 1: Collect Credentials
**Required**:
- SQL directory path
- Account name
- Username
- Database and schema names
- Warehouse name (defaults to `COMPUTE_WH`)

**Authentication Options**:
- Password (from argument, environment variable `SNOWFLAKE_PASSWORD`, or prompt)
- SSO (externalbrowser)
- Key-pair (using environment variables)

### Step 2: Execute Python Script
**Use Bash tool** with `run_in_background: true` to execute:
```bash
python3 /path/to/plugins/cdp-hybrid-idu/scripts/snowflake/snowflake_sql_executor.py \
    <sql_directory> \
    --account <account> \
    --user <user> \
    --database <database> \
    --schema <schema> \
    --warehouse <warehouse> \
    --password <password>
```

### Step 3: Monitor Execution in Real-Time
**Use BashOutput tool** to stream progress:
- Connection status
- File execution progress
- Row counts and timing
- Convergence detection results
- Error messages

**Display Progress**:
```
✓ Connected to Snowflake: <account>
• Using database: <database>, schema: <schema>

Executing: 01_create_graph.sql
✓ Completed: 01_create_graph.sql

Executing: 02_extract_merge.sql
✓ Completed: 02_extract_merge.sql
• Rows affected: 125,000

Executing Unify Loop (convergence detection)

--- Iteration 1 ---
✓ Iteration 1 completed
• Updated records: 1,500

--- Iteration 2 ---
✓ Iteration 2 completed
• Updated records: 450

--- Iteration 3 ---
✓ Iteration 3 completed
• Updated records: 0
✓ Loop converged after 3 iterations!

• Creating alias table: loop_final
...
```

### Step 4: Handle Interactive Prompts
If script encounters errors and prompts for continuation:
```
✗ Error in file: 04_unify_loop_iteration_01.sql
Error: Table not found

Continue with remaining files? (y/n):
```

**Agent Decision**:
1. Show error to user
2. Ask user for decision
3. Pass response to script

### Step 5: Final Report
**After completion**:
```
Execution Complete!

Summary:
  • Files processed: 18/18
  • Execution time: 45 minutes
  • Convergence: 3 iterations
  • Final lookup table rows: 98,500

Validation:
  ✓ All tables created successfully
  ✓ Canonical IDs generated
  ✓ Enriched tables populated
  ✓ Master tables created

Next Steps:
  1. Verify data quality
  2. Check coverage metrics
  3. Review statistics tables
```

## Critical Behaviors

### Convergence Monitoring
Track loop iterations:
- Iteration number
- Records updated
- Convergence status

### Error Recovery
On errors:
1. Capture error details
2. Determine severity (critical vs warning)
3. Prompt user for continuation decision
4. Log error for troubleshooting

### Performance Tracking
Monitor:
- Execution time per file
- Row counts processed
- Total workflow time

## MUST DO

1. **Stream output in real-time** using BashOutput
2. **Monitor convergence** and report iterations
3. **Handle user prompts** for error continuation
4. **Report final statistics** with coverage metrics
5. **Verify connection** before starting execution
6. **Clean up** on termination or error
