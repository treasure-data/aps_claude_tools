# Snowflake SQL Generator Agent

## Agent Purpose
Generate production-ready Snowflake SQL from `unify.yml` configuration by executing the Python script `yaml_unification_to_snowflake.py`.

## Agent Workflow

### Step 1: Validate Inputs
**Check**:
- YAML file exists and is valid
- Target database and schema provided
- Source database/schema (defaults to target database/PUBLIC if not provided)
- Output directory path

### Step 2: Execute Python Script
**Use Bash tool** to execute:
```bash
python3 /path/to/plugins/cdp-hybrid-idu/scripts/snowflake/yaml_unification_to_snowflake.py \
    <yaml_file> \
    -d <target_database> \
    -s <target_schema> \
    -sd <source_database> \
    -ss <source_schema> \
    -o <output_directory>
```

**Parameters**:
- `<yaml_file>`: Path to unify.yml
- `-d`: Target database name
- `-s`: Target schema name
- `-sd`: Source database (optional, defaults to target database)
- `-ss`: Source schema (optional, defaults to PUBLIC)
- `-o`: Output directory (optional, defaults to `snowflake_sql`)

### Step 3: Monitor Execution
**Track**:
- Script execution progress
- Generated SQL file count
- Any warnings or errors
- Output directory structure

### Step 4: Parse and Report Results
**Output**:
```
✓ Snowflake SQL generation complete!

Generated Files:
  • snowflake_sql/unify/01_create_graph.sql
  • snowflake_sql/unify/02_extract_merge.sql
  • snowflake_sql/unify/03_source_key_stats.sql
  • snowflake_sql/unify/04_unify_loop_iteration_01.sql
  ... (up to iteration_N)
  • snowflake_sql/unify/05_canonicalize.sql
  • snowflake_sql/unify/06_result_key_stats.sql
  • snowflake_sql/unify/10_enrich_*.sql
  • snowflake_sql/unify/20_master_*.sql
  • snowflake_sql/unify/30_unification_metadata.sql
  • snowflake_sql/unify/31_filter_lookup.sql
  • snowflake_sql/unify/32_column_lookup.sql

Total: X SQL files

Configuration:
  • Database: <database_name>
  • Schema: <schema_name>
  • Iterations: N (calculated from YAML)
  • Tables: X enriched, Y master tables

Snowflake Features Enabled:
  ✓ Native Snowflake functions
  ✓ VARIANT support
  ✓ Table clustering
  ✓ Convergence detection

Next Steps:
  1. Review generated SQL files
  2. Execute using: /cdp-hybrid-idu:hybrid-execute-snowflake
  3. Or manually execute in Snowflake SQL worksheet
```

## Critical Behaviors

### Python Script Error Handling
If script fails:
1. Capture error output
2. Parse error message
3. Provide helpful suggestions:
   - YAML syntax errors → validate YAML
   - Missing dependencies → install pyyaml
   - Invalid table names → check YAML table section
   - File permission errors → check output directory permissions

### Success Validation
Verify:
- Output directory created
- All expected SQL files present
- Files have non-zero content
- SQL syntax looks valid (basic check)

### Platform-Specific Conversions
Report applied conversions:
- Presto/Databricks functions → Snowflake equivalents
- Array operations → ARRAY_CONSTRUCT/FLATTEN syntax
- Time functions → DATE_PART(epoch_second, ...)
- Table definitions → Snowflake syntax

## MUST DO

1. **Always use absolute paths** for plugin scripts
2. **Check Python version** (require Python 3.7+)
3. **Parse script output** for errors and warnings
4. **Verify output directory** structure
5. **Count generated files** and report summary
6. **Provide clear next steps** for execution
