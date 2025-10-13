# Databricks Workflow Executor Agent

## Agent Purpose
Execute generated Databricks SQL workflow with intelligent convergence detection, real-time monitoring, and interactive error handling by orchestrating the Python script `databricks_sql_executor.py`.

## Agent Workflow

### Step 1: Collect Credentials
**Required**:
- SQL directory path
- Server hostname (e.g., `your-workspace.cloud.databricks.com`)
- HTTP path (e.g., `/sql/1.0/warehouses/abc123`)
- Catalog and schema names
- Authentication type (PAT or OAuth)

**For PAT Authentication**:
- Access token (from argument, environment variable `DATABRICKS_TOKEN`, or prompt)

**For OAuth**:
- No token required (browser-based auth)

### Step 2: Execute Python Script
**Use Bash tool** with `run_in_background: true` to execute:
```bash
python3 /path/to/plugins/cdp-hybrid-idu/scripts/databricks/databricks_sql_executor.py \
    <sql_directory> \
    --server-hostname <hostname> \
    --http-path <http_path> \
    --catalog <catalog> \
    --schema <schema> \
    --auth-type <pat|oauth> \
    --access-token <token> \
    --optimize-tables
```

### Step 3: Monitor Execution in Real-Time
**Use BashOutput tool** to stream progress:
- Connection status
- File execution progress
- Row counts and timing
- Convergence detection results
- Optimization status
- Error messages

**Display Progress**:
```
✓ Connected to Databricks: <hostname>
• Using catalog: <catalog>, schema: <schema>

Executing: 01_create_graph.sql
✓ Completed: 01_create_graph.sql

Executing: 02_extract_merge.sql
✓ Completed: 02_extract_merge.sql
• Rows affected: 125,000

Executing Unify Loop (convergence detection)

--- Iteration 1 ---
✓ Iteration 1 completed
• Updated records: 1,500
• Optimizing Delta table...

--- Iteration 2 ---
✓ Iteration 2 completed
• Updated records: 450
• Optimizing Delta table...

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
3. Pass response to script (via stdin if possible, or stop/restart)

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
- Optimization progress

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
- Optimization duration
- Total workflow time

## MUST DO

1. **Stream output in real-time** using BashOutput
2. **Monitor convergence** and report iterations
3. **Handle user prompts** for error continuation
4. **Report final statistics** with coverage metrics
5. **Verify connection** before starting execution
6. **Clean up** on termination or error
