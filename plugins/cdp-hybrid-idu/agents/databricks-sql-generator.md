# Databricks SQL Generator Agent

## Agent Purpose
Generate production-ready Databricks Delta Lake SQL from `unify.yml` configuration by executing the Python script `yaml_unification_to_databricks.py`.

## Agent Workflow

### Step 1: Validate Inputs
**Check**:
- YAML file exists and is valid
- Target catalog and schema provided
- Source catalog/schema (defaults to target if not provided)
- Output directory path

### Step 2: Execute Python Script
**Use Bash tool** to execute:
```bash
python3 /path/to/plugins/cdp-hybrid-idu/scripts/databricks/yaml_unification_to_databricks.py \
    <yaml_file> \
    -tc <target_catalog> \
    -ts <target_schema> \
    -sc <source_catalog> \
    -ss <source_schema> \
    -o <output_directory>
```

**Parameters**:
- `<yaml_file>`: Path to unify.yml
- `-tc`: Target catalog name
- `-ts`: Target schema name
- `-sc`: Source catalog (optional, defaults to target catalog)
- `-ss`: Source schema (optional, defaults to target schema)
- `-o`: Output directory (optional, defaults to `databricks_sql`)

### Step 3: Monitor Execution
**Track**:
- Script execution progress
- Generated SQL file count
- Any warnings or errors
- Output directory structure

### Step 4: Parse and Report Results
**Output**:
```
✓ Databricks SQL generation complete!

Generated Files:
  • databricks_sql/unify/01_create_graph.sql
  • databricks_sql/unify/02_extract_merge.sql
  • databricks_sql/unify/03_source_key_stats.sql
  • databricks_sql/unify/04_unify_loop_iteration_01.sql
  ... (up to iteration_N)
  • databricks_sql/unify/05_canonicalize.sql
  • databricks_sql/unify/06_result_key_stats.sql
  • databricks_sql/unify/10_enrich_*.sql
  • databricks_sql/unify/20_master_*.sql
  • databricks_sql/unify/30_unification_metadata.sql
  • databricks_sql/unify/31_filter_lookup.sql
  • databricks_sql/unify/32_column_lookup.sql

Total: X SQL files

Configuration:
  • Catalog: <catalog_name>
  • Schema: <schema_name>
  • Iterations: N (calculated from YAML)
  • Tables: X enriched, Y master tables

Delta Lake Features Enabled:
  ✓ ACID transactions
  ✓ Optimized clustering
  ✓ Convergence detection
  ✓ Performance optimizations

Next Steps:
  1. Review generated SQL files
  2. Execute using: /cdp-hybrid-idu:hybrid-execute-databricks
  3. Or manually execute in Databricks SQL editor
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
- Presto/Snowflake functions → Databricks equivalents
- Array operations → Spark SQL syntax
- Time functions → UNIX_TIMESTAMP()
- Table definitions → USING DELTA

## MUST DO

1. **Always use absolute paths** for plugin scripts
2. **Check Python version** (require Python 3.7+)
3. **Parse script output** for errors and warnings
4. **Verify output directory** structure
5. **Count generated files** and report summary
6. **Provide clear next steps** for execution
