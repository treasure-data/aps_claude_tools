---
name: transform-batch
description: Transform multiple database tables in parallel with maximum efficiency
---

# Transform Multiple Tables to Staging (Batch Mode)

## ⚠️ CRITICAL: This command enables parallel processing for 3x-10x faster transformations

I'll help you transform multiple database tables to staging format using parallel sub-agent execution for maximum performance.

---

## Required Information

Please provide the following details:

### 1. Source Tables
- **Table List**: Comma-separated list of tables (e.g., `table1, table2, table3`)
- **Format**: `database.table_name` or just `table_name` (if same database)
- **Example**: `client_src.customers_histunion, client_src.orders_histunion, client_src.products_histunion`

### 2. Source Configuration
- **Source Database**: Database containing tables (e.g., `client_src`)
- **Staging Database**: Target database (default: `client_stg`)
- **Lookup Database**: Reference database for rules (default: `client_config`)

### 3. SQL Engine (Optional)
- **Engine**: Choose one:
  - `presto` or `trino` - Presto/Trino SQL engine (default)
  - `hive` - Hive SQL engine
  - `mixed` - Specify engine per table
  - If not specified, will default to Presto/Trino for all tables

### 4. Mixed Engine Example (Optional)
If you need different engines for different tables:
```
Transform table1 using Hive, table2 using Presto, table3 using Hive
```

---

## What I'll Do

### Step 1: Parse Table List
I will extract individual tables from your input:
- Parse comma-separated list
- Detect database prefix for each table
- Identify total table count

### Step 2: Detect Engine Strategy
I will determine processing strategy:
- **Single Engine**: All tables use same engine
  - Presto/Trino (default) → All tables to `staging-transformer-presto`
  - Hive → All tables to `staging-transformer-hive`
- **Mixed Engines**: Different engines per table
  - Parse engine specification per table
  - Route each table to appropriate sub-agent

### Step 3: Launch Parallel Sub-Agents
I will create parallel sub-agent calls:
- **ONE sub-agent per table** (maximum parallelism)
- **Single message with multiple Task calls** (concurrent execution)
- **Each sub-agent processes independently** (no blocking)
- **All sub-agents skip git workflow** (consolidated at end)

### Step 4: Monitor Parallel Execution
I will track all sub-agent progress:
- Wait for all sub-agents to complete
- Collect results from each transformation
- Identify any failures or errors
- Report partial success if needed

### Step 5: Consolidate Results
After ALL tables complete successfully:
1. **Aggregate file changes** across all tables
2. **Execute single git workflow**:
   - Create feature branch
   - Commit all changes together
   - Push to remote
   - Create comprehensive PR
3. **Report complete summary**

---

## Processing Strategy

### Parallel Processing (Recommended for 2+ Tables)
```
User requests: "Transform tables A, B, C"

Main Claude creates 3 parallel sub-agent calls:

┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│  Sub-Agent 1    │  │  Sub-Agent 2    │  │  Sub-Agent 3    │
│  (Table A)      │  │  (Table B)      │  │  (Table C)      │
│  staging-       │  │  staging-       │  │  staging-       │
│  transformer-   │  │  transformer-   │  │  transformer-   │
│  presto         │  │  presto         │  │  presto         │
└─────────────────┘  └─────────────────┘  └─────────────────┘
        ↓                     ↓                     ↓
    [Files for A]        [Files for B]        [Files for C]
        ↓                     ↓                     ↓
        └─────────────────────┴─────────────────────┘
                              ↓
                    [Consolidated Git Workflow]
                    [Single PR with all tables]
```

### Performance Benefits:
- **Speed**: N tables in ~1x time instead of N×time
- **Efficiency**: Optimal resource utilization
- **User Experience**: Faster results for batch operations
- **Scalability**: Can handle 10+ tables efficiently

---

## Quality Assurance (Per Table)

Each sub-agent ensures complete compliance:

✅ **Column Limit Management** (max 200 columns)
✅ **JSON Detection & Extraction** (automatic)
✅ **Date Processing** (4 outputs per date column)
✅ **Email/Phone Validation** (with hashing)
✅ **String Standardization** (UPPER, TRIM, NULL handling)
✅ **Deduplication Logic** (if configured)
✅ **Join Processing** (if specified)
✅ **Incremental Processing** (state tracking)
✅ **SQL File Creation** (init, incremental, upsert)
✅ **DIG File Management** (conditional creation)
✅ **Configuration Update** (src_params.yml)
✅ **Treasure Data Compatibility** (VARCHAR/BIGINT timestamps)

---

## Output Files

### For Presto/Trino Engine (per table):
- `staging/init_queries/{source_db}_{table}_init.sql`
- `staging/queries/{source_db}_{table}.sql`
- `staging/queries/{source_db}_{table}_upsert.sql` (if dedup)
- Updated `staging/config/src_params.yml` (all tables)
- `staging/staging_transformation.dig` (created once if not exists)

### For Hive Engine (per table):
- `staging_hive/queries/{source_db}_{table}.sql`
- Updated `staging_hive/config/src_params.yml` (all tables)
- `staging_hive/staging_hive.dig` (created once if not exists)
- Template files (created once if not exist)

### Plus:
- Single git commit with all tables
- Comprehensive pull request
- Complete validation report for all tables

---

## Example Usage

### Example 1: Same Engine (Presto Default)
```
User: Transform tables: client_src.customers_histunion, client_src.orders_histunion, client_src.products_histunion

→ Parallel execution with 3 staging-transformer-presto agents
→ All files to staging/ directory
→ Single consolidated git workflow
→ Time: ~1x (vs 3x sequential)
```

### Example 2: Same Engine (Hive Explicit)
```
User: Transform tables using Hive: client_src.events_histunion, client_src.profiles_histunion

→ Parallel execution with 2 staging-transformer-hive agents
→ All files to staging_hive/ directory
→ Single consolidated git workflow
→ Time: ~1x (vs 2x sequential)
```

### Example 3: Mixed Engines
```
User: Transform table1 using Hive, table2 using Presto, table3 using Hive

→ Parallel execution:
  - Table1 → staging-transformer-hive
  - Table2 → staging-transformer-presto
  - Table3 → staging-transformer-hive
→ Files distributed to appropriate directories
→ Single consolidated git workflow
→ Time: ~1x (vs 3x sequential)
```

---

## Error Handling

### Partial Success Scenario
If some tables succeed and others fail:

1. **Report Clear Status**:
   ```
   ✅ Successfully transformed: table1, table2
   ❌ Failed: table3 (error message)
   ```

2. **Preserve Successful Work**:
   - Keep files from successful transformations
   - Allow retry of only failed tables

3. **Git Safety**:
   - Only execute git workflow if ALL tables succeed
   - Otherwise, keep changes local for review

### Full Failure Scenario
If all tables fail:
- Report detailed error for each table
- No git workflow execution
- Provide troubleshooting guidance

---

## Next Steps After Batch Transformation

1. **Review Pull Request**:
   ```
   Title: "Batch transform 5 tables to staging"

   Body:
   - Transformed tables: table1, table2, table3, table4, table5
   - Engine: Presto/Trino
   - All validation gates passed ✅
   - Files created: 15 SQL files, 1 config update
   ```

2. **Verify Generated Files**:
   ```bash
   # For Presto
   ls -l staging/queries/
   ls -l staging/init_queries/
   cat staging/config/src_params.yml

   # For Hive
   ls -l staging_hive/queries/
   cat staging_hive/config/src_params.yml
   ```

3. **Test Workflow**:
   ```bash
   cd staging  # or staging_hive
   td wf push
   td wf run staging_transformation.dig  # or staging_hive.dig
   ```

4. **Monitor All Tables**:
   ```sql
   SELECT table_name, inc_value, project_name
   FROM client_config.inc_log
   WHERE table_name IN ('table1', 'table2', 'table3')
   ORDER BY inc_value DESC
   ```

---

## Performance Comparison

| Tables | Sequential Time | Parallel Time | Speedup |
|--------|----------------|---------------|---------|
| 2      | ~10 min        | ~5 min        | 2x      |
| 3      | ~15 min        | ~5 min        | 3x      |
| 5      | ~25 min        | ~5 min        | 5x      |
| 10     | ~50 min        | ~5 min        | 10x     |

**Note**: Actual times vary based on table complexity and data volume.

---

## Production-Ready Guarantee

All batch transformations will:
- ✅ Execute in parallel for maximum speed
- ✅ Maintain complete quality for each table
- ✅ Provide atomic git workflow (all or nothing)
- ✅ Include comprehensive error handling
- ✅ Generate maintainable code
- ✅ Match production standards exactly

---

**Ready to proceed? Please provide your table list and I'll launch parallel sub-agents for maximum efficiency!**

**Format Examples:**
- `Transform tables: table1, table2, table3` (same database)
- `Transform client_src.table1, client_src.table2` (explicit database)
- `Transform table1 using Hive, table2 using Presto` (mixed engines)
