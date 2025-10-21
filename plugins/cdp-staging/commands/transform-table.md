---
name: transform-table
description: Transform a single database table to staging format with data quality improvements, PII handling, and JSON extraction
---

# Transform Single Table to Staging

## ⚠️ CRITICAL: This command enforces strict sub-agent delegation

I'll help you transform a database table to staging format using the appropriate staging-transformer sub-agent (Presto/Trino or Hive).

---

## Required Information

Please provide the following details:

### 1. Source Table
- **Database Name**: Source database (e.g., `client_src`, `demo_db`)
- **Table Name**: Source table name (e.g., `customer_profiles_histunion`)

### 2. Target Configuration
- **Staging Database**: Target database (default: `client_stg`)
- **Lookup Database**: Reference database for rules (default: `config_db`)

### 3. SQL Engine (Optional)
- **Engine**: Choose one:
  - `presto` or `trino` - Presto/Trino SQL engine (default)
  - `hive` - Hive SQL engine
  - If not specified, will default to Presto/Trino

### 4. Transformation Requirements (Optional)
- **Deduplication**: Required? (will check config_db.staging_trnsfrm_rules)
- **JSON Columns**: Will auto-detect and process
- **Join Logic**: Any joins needed? (will check additional_rules)

---

## What I'll Do

### Step 1: Detect SQL Engine
I will determine the appropriate sub-agent:
- **Presto/Trino keywords** → `staging-transformer-presto`
- **Hive keywords** → `staging-transformer-hive`
- **No specification** → `staging-transformer-presto` (default)

### Step 2: Delegate to Specialized Agent
I will invoke the appropriate staging-transformer agent with:
- Complete table transformation context
- All mandatory requirements (13 rules)
- Engine-specific SQL generation
- Full compliance validation

### Step 3: Sub-Agent Will Execute
The specialized agent will:
1. **Validate table existence** (MANDATORY first step)
2. **Analyze metadata** (columns, types, data samples)
3. **Check configuration** (deduplication rules, additional rules)
4. **Detect JSON columns** (automatic processing)
5. **Generate SQL files**:
   - `staging/init_queries/{source_db}_{table_name}_init.sql` (Presto)
   - `staging/queries/{source_db}_{table_name}.sql` (Presto)
   - `staging/queries/{source_db}_{table_name}_upsert.sql` (if dedup, Presto)
   - OR `staging_hive/queries/{source_db}_{table_name}.sql` (Hive)
6. **Update configuration**: `staging/config/src_params.yml` or `staging_hive/config/src_params.yml`
7. **Create/verify DIG file**: `staging/staging_transformation.dig` or `staging_hive/staging_hive.dig`
8. **Execute git workflow**: Commit, branch, push, PR creation

---

## Quality Assurance

The sub-agent ensures complete compliance with all requirements:

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
✅ **Git Workflow** (complete automation)
✅ **Treasure Data Compatibility** (VARCHAR/BIGINT timestamps)

---

## Output Files

### For Presto/Trino Engine:
1. `staging/init_queries/{source_db}_{table_name}_init.sql` - Initial load SQL
2. `staging/queries/{source_db}_{table_name}.sql` - Incremental SQL
3. `staging/queries/{source_db}_{table_name}_upsert.sql` - Upsert SQL (if dedup)
4. `staging/config/src_params.yml` - Updated configuration
5. `staging/staging_transformation.dig` - Workflow (created if not exists)

### For Hive Engine:
1. `staging_hive/queries/{source_db}_{table_name}.sql` - Combined SQL
2. `staging_hive/config/src_params.yml` - Updated configuration
3. `staging_hive/staging_hive.dig` - Workflow (created if not exists)
4. `staging_hive/queries/get_max_time.sql` - Template (created if not exists)
5. `staging_hive/queries/get_stg_rows_for_delete.sql` - Template (created if not exists)

### Plus:
- Git commit with comprehensive message
- Pull request with transformation summary
- Validation report

---

## Example Usage

### Example 1: Presto Engine (Default)
```
User: Transform table client_src.customer_profiles_histunion
→ Engine: Presto (default)
→ Sub-agent: staging-transformer-presto
→ Output: staging/ directory files
```

### Example 2: Hive Engine (Explicit)
```
User: Transform table client_src.klaviyo_events_histunion using Hive
→ Engine: Hive
→ Sub-agent: staging-transformer-hive
→ Output: staging_hive/ directory files
```

### Example 3: With Custom Databases
```
User: Transform demo_db.orders_histunion
      Use demo_db_staging as staging database
      Use config_db for lookup
→ Engine: Presto (default)
→ Custom databases applied
```

---

## Next Steps After Transformation

1. **Review generated files**:
   ```bash
   ls -l staging/queries/
   ls -l staging/init_queries/
   cat staging/config/src_params.yml
   ```

2. **Review Pull Request**:
   - Check transformation summary
   - Verify all validation gates passed
   - Review generated SQL

3. **Test the workflow**:
   ```bash
   cd staging
   td wf push
   td wf run staging_transformation.dig
   ```

4. **Monitor execution**:
   ```sql
   SELECT * FROM config_db.inc_log
   WHERE table_name = '{your_table}'
   ORDER BY inc_value DESC
   LIMIT 1
   ```

---

## Production-Ready Guarantee

All transformations will:
- ✅ Work the first time
- ✅ Follow consistent patterns
- ✅ Include complete error handling
- ✅ Include comprehensive data quality
- ✅ Be maintainable and documented
- ✅ Match production standards exactly

---

**Ready to proceed? Please provide the source database and table name, and I'll delegate to the appropriate staging-transformer agent for complete processing.**
