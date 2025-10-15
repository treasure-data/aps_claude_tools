---
name: hybrid-generate-snowflake
description: Generate Snowflake SQL from YAML configuration for ID unification
---

# Generate Snowflake SQL from YAML

## Overview

Generate production-ready Snowflake SQL workflow from your `unify.yml` configuration file. This command creates Snowflake-native SQL files with proper clustering, VARIANT support, and platform-specific function conversions.

---

## What You Need

### Required Inputs
1. **YAML Configuration File**: Path to your `unify.yml`
2. **Target Database**: Snowflake database name
3. **Target Schema**: Schema name within the database

### Optional Inputs
4. **Source Database**: Database containing source tables (defaults to target database)
5. **Source Schema**: Schema containing source tables (defaults to PUBLIC)
6. **Output Directory**: Where to save generated SQL (defaults to `snowflake_sql/`)

---

## What I'll Do

### Step 1: Validation
- Verify `unify.yml` exists and is valid
- Check YAML syntax and structure
- Validate keys, tables, and configuration sections

### Step 2: SQL Generation
I'll call the **snowflake-sql-generator agent** to:
- Execute `yaml_unification_to_snowflake.py` Python script
- Generate Snowflake table definitions with clustering
- Create convergence detection logic
- Build cryptographic hashing for canonical IDs

### Step 3: Output Organization
Generate complete SQL workflow in this structure:
```
snowflake_sql/unify/
├── 01_create_graph.sql              # Initialize graph table
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
- Snowflake optimizations included
- Key features enabled
- Next steps for execution

---

## Command Usage

### Basic Usage
```
/cdp-hybrid-idu:hybrid-generate-snowflake

I'll prompt you for:
- YAML file path
- Target database
- Target schema
```

### Advanced Usage
Provide all parameters upfront:
```
YAML file: /path/to/unify.yml
Target database: my_database
Target schema: my_schema
Source database: source_database (optional)
Source schema: PUBLIC (optional, defaults to PUBLIC)
Output directory: custom_output/ (optional)
```

---

## Generated SQL Features

### Snowflake Optimizations
- **Clustering**: `CLUSTER BY (follower_id)` on graph tables
- **VARIANT Support**: Flexible data structures for arrays and objects
- **Native Functions**: Snowflake-specific optimized functions

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
   - `valid_regexp`: REGEXP_LIKE pattern filtering
   - `invalid_texts`: NOT IN clause with proper NULL handling
   - Combined AND logic for strict validation

4. **Master Table Attributes**:
   - Single value: `MAX_BY(attr, order)` with COALESCE
   - Array values: `ARRAY_SLICE(ARRAY_CAT(arrays), 0, N)`
   - Priority-based selection

### Platform-Specific Conversions
The generator automatically converts:
- Presto functions → Snowflake equivalents
- Databricks functions → Snowflake equivalents
- Array operations → ARRAY_CONSTRUCT/FLATTEN syntax
- Window functions → optimized versions
- Time functions → DATE_PART(epoch_second, CURRENT_TIMESTAMP())

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
snowflake_sql/unify/
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
/cdp-hybrid-idu:hybrid-execute-snowflake
```

### Option 2: Review First
1. Examine generated SQL files
2. Verify table names and transformations
3. Test with sample data
4. Execute manually or via execution command

### Option 3: Customize
1. Modify generated SQL as needed
2. Add custom logic or transformations
3. Execute using Snowflake SQL worksheet or execution command

---

## Technical Details

### Python Script Execution
The agent executes:
```bash
python3 scripts/snowflake/yaml_unification_to_snowflake.py \
    unify.yml \
    -d my_database \
    -s my_schema \
    -sd source_database \
    -ss source_schema \
    -o snowflake_sql
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

### Snowflake-Specific Features
- **LATERAL FLATTEN**: Array expansion for id_ns_array processing
- **ARRAY_CONSTRUCT**: Building arrays from multiple columns
- **OBJECT_CONSTRUCT**: Creating structured objects for key-value pairs
- **ARRAYS_OVERLAP**: Checking array membership
- **SPLIT_PART**: String splitting for leader key parsing

---

## Troubleshooting

### Common Issues

**Issue**: YAML validation error
**Solution**: Check YAML syntax, ensure proper indentation, verify all required fields

**Issue**: Table not found error
**Solution**: Verify source database/schema, check table names in YAML

**Issue**: Python script error
**Solution**: Ensure Python 3.7+ installed, check pyyaml dependency

**Issue**: Too many/few iterations
**Solution**: Adjust `merge_iterations` in canonical_ids section of YAML

**Issue**: VARIANT column errors
**Solution**: Snowflake VARIANT type handling is automatic, ensure proper casting in custom SQL

---

## Success Criteria

Generated SQL will:
- ✅ Be valid Snowflake SQL
- ✅ Use native Snowflake functions
- ✅ Include proper clustering for performance
- ✅ Have convergence detection built-in
- ✅ Support VARIANT types for flexible data
- ✅ Generate comprehensive statistics
- ✅ Work without modification on Snowflake

---

**Ready to generate Snowflake SQL from your YAML configuration?**

Provide your YAML file path and target database/schema to begin!
