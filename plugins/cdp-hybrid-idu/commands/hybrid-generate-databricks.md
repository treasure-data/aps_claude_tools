---
name: hybrid-generate-databricks
description: Generate Databricks Delta Lake SQL from YAML configuration for ID unification
---

# Generate Databricks SQL from YAML

## Overview

Generate production-ready Databricks SQL workflow from your `unify.yml` configuration file. This command creates Delta Lake optimized SQL files with ACID transactions, clustering, and platform-specific function conversions.

---

## What You Need

### Required Inputs
1. **YAML Configuration File**: Path to your `unify.yml`
2. **Target Catalog**: Databricks Unity Catalog name
3. **Target Schema**: Schema name within the catalog

### Optional Inputs
4. **Source Catalog**: Catalog containing source tables (defaults to target catalog)
5. **Source Schema**: Schema containing source tables (defaults to target schema)
6. **Output Directory**: Where to save generated SQL (defaults to `databricks_sql/`)

---

## What I'll Do

### Step 1: Validation
- Verify `unify.yml` exists and is valid
- Check YAML syntax and structure
- Validate keys, tables, and configuration sections

### Step 2: SQL Generation
I'll call the **databricks-sql-generator agent** to:
- Execute `yaml_unification_to_databricks.py` Python script
- Apply Databricks-specific SQL conversions:
  - `ARRAY_SIZE` → `SIZE`
  - `ARRAY_CONSTRUCT` → `ARRAY`
  - `OBJECT_CONSTRUCT` → `STRUCT`
  - `COLLECT_LIST` for aggregations
  - `FLATTEN` for array operations
  - `UNIX_TIMESTAMP()` for time functions
- Generate Delta Lake table definitions with clustering
- Create convergence detection logic
- Build cryptographic hashing for canonical IDs

### Step 3: Output Organization
Generate complete SQL workflow in this structure:
```
databricks_sql/unify/
├── 01_create_graph.sql              # Initialize graph with USING DELTA
├── 02_extract_merge.sql             # Extract identities with validation
├── 03_source_key_stats.sql          # Source statistics with GROUPING SETS
├── 04_unify_loop_iteration_*.sql    # Loop iterations (auto-calculated count)
├── 05_canonicalize.sql              # Canonical ID creation with key masks
├── 06_result_key_stats.sql          # Result statistics with histograms
├── 10_enrich_*.sql                  # Enrich each source table
├── 20_master_*.sql                  # Master tables with attribute aggregation
├── 30_unification_metadata.sql      # Metadata tables
├── 31_filter_lookup.sql             # Validation rules lookup
└── 32_column_lookup.sql             # Column mapping lookup
```

### Step 4: Summary Report
Provide:
- Total SQL files generated
- Estimated execution order
- Delta Lake optimizations included
- Key features enabled
- Next steps for execution

---

## Command Usage

### Basic Usage
```
/cdp-hybrid-idu:hybrid-generate-databricks

I'll prompt you for:
- YAML file path
- Target catalog
- Target schema
```

### Advanced Usage
Provide all parameters upfront:
```
YAML file: /path/to/unify.yml
Target catalog: my_catalog
Target schema: my_schema
Source catalog: source_catalog (optional)
Source schema: source_schema (optional)
Output directory: custom_output/ (optional)
```

---

## Generated SQL Features

### Delta Lake Optimizations
- **ACID Transactions**: `USING DELTA` for all tables
- **Clustering**: `CLUSTER BY (follower_id)` on graph tables
- **Table Properties**: Optimized for large-scale joins

### Advanced Capabilities
1. **Dynamic Iteration Count**: Auto-calculates based on:
   - Number of merge keys
   - Number of tables
   - Data complexity (configurable via YAML)

2. **Key-Specific Hashing**: Each key uses unique cryptographic mask:
   ```
   Key Type 1 (email):       0ffdbcf0c666ce190d
   Key Type 2 (customer_id): 61a821f2b646a4e890
   Key Type 3 (phone):       acd2206c3f88b3ee27
   ```

3. **Validation Rules**:
   - `valid_regexp`: Regex pattern filtering
   - `invalid_texts`: NOT IN clause with NULL handling
   - Combined AND logic for strict validation

4. **Master Table Attributes**:
   - Single value: `MAX_BY(attr, order)` with COALESCE
   - Array values: `SLICE(CONCAT(arrays), 1, N)`
   - Priority-based selection

### Platform-Specific Conversions
The generator automatically converts:
- Presto functions → Databricks equivalents
- Snowflake functions → Databricks equivalents
- Array operations → Spark SQL syntax
- Window functions → optimized versions
- Time functions → UNIX_TIMESTAMP()

---

## Example Workflow

### Input YAML (`unify.yml`)
```yaml
name: customer_unification

keys:
  - name: email
    valid_regexp: ".*@.*"
    invalid_texts: ['', 'N/A', 'null']
  - name: customer_id
    invalid_texts: ['', 'N/A']

tables:
  - table: customer_profiles
    key_columns:
      - {column: email_std, key: email}
      - {column: customer_id, key: customer_id}

canonical_ids:
  - name: unified_id
    merge_by_keys: [email, customer_id]
    merge_iterations: 15

master_tables:
  - name: customer_master
    canonical_id: unified_id
    attributes:
      - name: best_email
        source_columns:
          - {table: customer_profiles, column: email_std, priority: 1}
```

### Generated Output
```
databricks_sql/unify/
├── 01_create_graph.sql              # Creates unified_id_graph_unify_loop_0
├── 02_extract_merge.sql             # Merges customer_profiles keys
├── 03_source_key_stats.sql          # Stats by table
├── 04_unify_loop_iteration_01.sql   # First iteration
├── 04_unify_loop_iteration_02.sql   # Second iteration
├── ...                              # Up to iteration_05
├── 05_canonicalize.sql              # Creates unified_id_lookup
├── 06_result_key_stats.sql          # Final statistics
├── 10_enrich_customer_profiles.sql  # Adds unified_id column
├── 20_master_customer_master.sql    # Creates customer_master table
├── 30_unification_metadata.sql      # Metadata
├── 31_filter_lookup.sql             # Validation rules
└── 32_column_lookup.sql             # Column mappings
```

---

## Next Steps After Generation

### Option 1: Execute Immediately
Use the execution command:
```
/cdp-hybrid-idu:hybrid-execute-databricks
```

### Option 2: Review First
1. Examine generated SQL files
2. Verify table names and transformations
3. Test with sample data
4. Execute manually or via execution command

### Option 3: Customize
1. Modify generated SQL as needed
2. Add custom logic or transformations
3. Execute using Databricks SQL editor or execution command

---

## Technical Details

### Python Script Execution
The agent executes:
```bash
python3 scripts/databricks/yaml_unification_to_databricks.py \
    unify.yml \
    -tc my_catalog \
    -ts my_schema \
    -sc source_catalog \
    -ss source_schema \
    -o databricks_sql
```

### SQL File Naming Convention
- `01-09`: Setup and initialization
- `10-19`: Source table enrichment
- `20-29`: Master table creation
- `30-39`: Metadata and lookup tables
- `04_*_NN`: Loop iterations (auto-numbered)

### Convergence Detection
Each loop iteration includes:
```sql
-- Check if graph changed
SELECT COUNT(*) FROM (
    SELECT leader_ns, leader_id, follower_ns, follower_id
    FROM iteration_N
    EXCEPT
    SELECT leader_ns, leader_id, follower_ns, follower_id
    FROM iteration_N_minus_1
) diff
```
Stops when count = 0

---

## Troubleshooting

### Common Issues

**Issue**: YAML validation error
**Solution**: Check YAML syntax, ensure proper indentation, verify all required fields

**Issue**: Table not found error
**Solution**: Verify source catalog/schema, check table names in YAML

**Issue**: Python script error
**Solution**: Ensure Python 3.7+ installed, check pyyaml dependency

**Issue**: Too many/few iterations
**Solution**: Adjust `merge_iterations` in canonical_ids section of YAML

---

## Success Criteria

Generated SQL will:
- ✅ Be valid Databricks Spark SQL
- ✅ Use Delta Lake for ACID transactions
- ✅ Include proper clustering for performance
- ✅ Have convergence detection built-in
- ✅ Support incremental processing
- ✅ Generate comprehensive statistics
- ✅ Work without modification on Databricks

---

**Ready to generate Databricks SQL from your YAML configuration?**

Provide your YAML file path and target catalog/schema to begin!
