# CLAUDE.md - Hybrid ID Unification (Snowflake + Databricks) Plugin

## Project Overview
This Claude Code plugin provides **multi-platform ID Unification** supporting both Snowflake and Databricks, enabling teams to unify customer identities across hybrid cloud environments with YAML-driven configuration.

**Purpose**: Generate and execute production-ready ID unification workflows for Snowflake and Databricks with convergence detection, cryptographic hashing, master table creation, and comprehensive monitoring.

## Why Hybrid IDU Plugin?

### The Challenge
Many organizations use **both Snowflake and Databricks** for different use cases:
- **Snowflake**: Data warehousing, BI, reporting
- **Databricks**: ML/AI, real-time processing, Delta Lake

**Traditional approaches** require:
- Separate unification pipelines for each platform
- Manual SQL conversion between dialects
- Different execution frameworks
- Duplicate configuration management

### The Solution
This plugin provides a **unified interface** to:
1. Create **single YAML configuration** that works for both platforms
2. Generate **platform-specific SQL** optimized for each engine
3. Execute workflows with **intelligent convergence detection**
4. Monitor execution with **real-time progress tracking**
5. Share configurations **across teams** via Claude Code

## Core Capabilities

### 1. YAML-Driven Configuration
Create a single `unify.yml` that defines:
- **Keys**: User identifier columns (email, phone, td_client_id, etc.)
- **Tables**: Source tables with key mappings
- **Validation**: Regex patterns and invalid value filtering
- **Canonical IDs**: Merge strategies and priorities
- **Master Tables**: Attribute aggregation with conflict resolution

### 2. Multi-Platform SQL Generation
**Databricks (Delta Lake)**:
- ACID transactions for data integrity
- Optimized clustering on key columns
- Platform-specific function conversion (SIZE, ARRAY, STRUCT, etc.)
- URL-safe base64 encoding for canonical IDs
- Delta table optimization

**Snowflake**:
- Snowflake-native functions (ARRAY_CONSTRUCT, OBJECT_CONSTRUCT, etc.)
- VARIANT support for flexible data structures
- Proper NULL handling with Snowflake semantics
- Table clustering for performance
- LATERAL FLATTEN for array operations

### 3. Advanced Unification Features
- **Dynamic Loop Iterations**: Auto-calculates optimal iteration count based on data complexity
- **Convergence Detection**: Stops iterations when graph stabilizes (no more updates)
- **Key-Specific Hashing**: Each key type uses unique cryptographic masks (supports up to 10 keys)
- **Priority-Based Merging**: Configurable hierarchy for identity resolution
- **Transitive Closure**: Full graph traversal for complete identity resolution
- **Master Table Generation**: Automated customer profile creation with attribute aggregation

### 4. Intelligent Execution
- **Real-time Monitoring**: Progress tracking with row counts and execution times
- **Error Recovery**: Intelligent checkpointing and retry mechanisms
- **Interactive Prompts**: Continue/stop decisions on errors
- **Optimization**: Auto-optimization of Delta tables (Databricks)
- **Alias Tables**: Creates final iteration aliases for downstream processing

## Plugin Architecture

### Specialized Agents
1. **yaml-configuration-builder**: Interactive YAML creation with validation
2. **databricks-sql-generator**: Calls `yaml_unification_to_databricks.py` to generate SQL
3. **snowflake-sql-generator**: Calls `yaml_unification_to_snowflake.py` to generate SQL
4. **databricks-workflow-executor**: Orchestrates `databricks_sql_executor.py` with monitoring
5. **snowflake-workflow-executor**: Orchestrates `snowflake_sql_executor.py` with monitoring

### Slash Commands
1. `/cdp-hybrid-idu:hybrid-setup` - Complete end-to-end setup wizard
2. `/cdp-hybrid-idu:hybrid-generate-databricks` - Generate Databricks SQL from YAML
3. `/cdp-hybrid-idu:hybrid-generate-snowflake` - Generate Snowflake SQL from YAML
4. `/cdp-hybrid-idu:hybrid-execute-databricks` - Execute Databricks workflow
5. `/cdp-hybrid-idu:hybrid-execute-snowflake` - Execute Snowflake workflow
6. `/cdp-hybrid-idu:hybrid-unif-config-validate` - Validate YAML configuration

## Generated SQL Workflow Structure

Both platforms generate a consistent workflow structure:

```
01_create_graph.sql              # Initialize identity graph table
02_extract_merge.sql             # Extract and merge identity data
03_source_key_stats.sql          # Generate source statistics
04_unify_loop_iteration_*.sql    # Iterative graph unification (convergence-based)
05_canonicalize.sql              # Create canonical ID mappings
06_result_key_stats.sql          # Final unification statistics
10_enrich_*.sql                  # Enrich source tables with unified IDs
20_master_*.sql                  # Generate master customer tables
30_unification_metadata.sql      # Create metadata tables
31_filter_lookup.sql             # Key validation rules
32_column_lookup.sql             # Column mapping metadata
```

## YAML Configuration Example

```yaml
name: customer_unification

keys:
  - name: email
    valid_regexp: ".*@.*"          # Optional: regex validation
    invalid_texts: ['', 'N/A', 'null', 'unknown']
  - name: customer_id
    invalid_texts: ['', 'N/A', 'null']
  - name: phone_number
    invalid_texts: ['', 'N/A', 'null']

tables:
  - table: customer_profiles
    key_columns:
      - {column: email_std, key: email}
      - {column: customer_id, key: customer_id}
  - table: customer_staging
    key_columns:
      - {column: email, key: email}
  - table: orders
    key_columns:
      - {column: email_address, key: email}
      - {column: phone_number, key: phone_number}

canonical_ids:
  - name: unified_id
    merge_by_keys: [email, customer_id, phone_number]
    merge_iterations: 15  # Optional: auto-calculated if not specified

master_tables:
  - name: customer_master
    canonical_id: unified_id
    attributes:
      - name: best_email
        source_columns:
          - {table: customer_profiles, column: email_std, priority: 1, order_by: time}
          - {table: orders, column: email_address, priority: 2, order_by: time}
      - name: top_3_emails
        array_elements: 3
        source_columns:
          - {table: customer_profiles, column: email_std, priority: 1, order_by: time}
          - {table: orders, column: email_address, priority: 2, order_by: time}
```

## How to Use This Plugin

### Quick Start: Complete Setup
```
/cdp-hybrid-idu:hybrid-setup

Follow the interactive wizard:
1. Provide table list
2. Define keys and validation rules
3. Choose platform (Databricks, Snowflake, or both)
4. Configure master tables (optional)
5. Generate SQL workflows
6. Execute with monitoring
```

### Generate SQL Only
```
# For Databricks
/cdp-hybrid-idu:hybrid-generate-databricks

Inputs:
- YAML file path: unify.yml
- Target catalog: my_catalog
- Target schema: my_schema
- Source catalog (optional): src_catalog
- Source schema (optional): src_schema

# For Snowflake
/cdp-hybrid-idu:hybrid-generate-snowflake

Inputs:
- YAML file path: unify.yml
- Target database: my_database
- Target schema: my_schema
- Source schema (optional): PUBLIC
```

### Execute Workflow
```
# For Databricks
/cdp-hybrid-idu:hybrid-execute-databricks

Requires:
- SQL directory: databricks_sql/unify/
- Server hostname: your-workspace.databricks.com
- HTTP path: /sql/1.0/warehouses/your-warehouse-id
- Catalog and schema names
- Authentication: PAT or OAuth

# For Snowflake
/cdp-hybrid-idu:hybrid-execute-snowflake

Requires:
- SQL directory: snowflake_sql/unify/
- Account name: your-account
- User credentials
- Database and schema names
- Warehouse name
```

### Validate Configuration
```
/cdp-hybrid-idu:hybrid-unif-config-validate

Checks:
- YAML syntax and structure
- Key definitions and relationships
- Table references
- Validation rules
- Master table configurations
```

## Key Features Deep Dive

### 1. Advanced Data Validation
```yaml
keys:
  - name: email
    valid_regexp: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
    invalid_texts: ['', 'N/A', 'null', 'unknown', 'test@test.com']
```

**How it works**:
- `valid_regexp`: Filters rows where column value matches regex pattern
- `invalid_texts`: Filters rows using `NOT IN` clause (handles NULL properly)
- Combined: Both conditions applied with AND logic

### 2. Convergence Detection
**Algorithm**:
```sql
-- Check if iteration N changed anything from iteration N-1
SELECT COUNT(*) FROM (
    SELECT leader_ns, leader_id, follower_ns, follower_id
    FROM iteration_N
    EXCEPT
    SELECT leader_ns, leader_id, follower_ns, follower_id
    FROM iteration_N_minus_1
) diff
```

**Stops when**: `COUNT(*) = 0` (no changes)

### 3. Cryptographic Hashing
Each key type uses a unique mask for canonical ID generation:
- **Key Type 1** (email): `0ffdbcf0c666ce190d`
- **Key Type 2** (customer_id): `61a821f2b646a4e890`
- **Key Type 3** (phone): `acd2206c3f88b3ee27`
- **Supports up to 10 key types**

**Formula**: `BASE64(CONCAT(XOR(SHA256(id), mask_low), mask_high))`

### 4. Master Table Attributes
**Single Value**:
```yaml
- name: best_email
  source_columns:
    - {table: profiles, column: email, priority: 1}
    - {table: orders, column: email_address, priority: 2}
```
Uses `MAX_BY(attr, order_by)` with priority-based `COALESCE`

**Array Values**:
```yaml
- name: top_3_emails
  array_elements: 3
  source_columns:
    - {table: profiles, column: email, priority: 1}
    - {table: orders, column: email_address, priority: 2}
```
Uses `SLICE(CONCAT(arrays), 1, 3)` with aggregation

## Platform-Specific Optimizations

### Databricks (Delta Lake)
- **ACID Transactions**: `USING DELTA` tables
- **Clustering**: `CLUSTER BY (follower_id)` for join optimization
- **Optimization**: `OPTIMIZE table_name` after major operations
- **Functions**: `SIZE()`, `COLLECT_LIST()`, `STRUCT()`, `FLATTEN()`
- **Time**: `UNIX_TIMESTAMP()` for epoch seconds

### Snowflake
- **Clustering**: `CLUSTER BY (follower_id)` for performance
- **Functions**: `ARRAY_SIZE()`, `ARRAY_AGG()`, `OBJECT_CONSTRUCT()`, `FLATTEN()`
- **Arrays**: `LATERAL FLATTEN(input => array)` for array operations
- **Time**: `DATE_PART(epoch_second, CURRENT_TIMESTAMP())`
- **Variants**: VARIANT type for flexible data structures

## Execution Workflow

### 1. Setup Phase
- Create graph table (loop_0)
- Extract identities from source tables
- Generate source statistics

### 2. Unification Loop
```
Iteration 1:
  - Execute unify SQL
  - Check convergence: 1500 updates
  - Continue...

Iteration 2:
  - Execute unify SQL
  - Check convergence: 450 updates
  - Continue...

Iteration 3:
  - Execute unify SQL
  - Check convergence: 0 updates
  - CONVERGED! Stop loop
```

### 3. Finalization Phase
- Create alias table (loop_final → loop_3)
- Canonicalize (create lookup tables)
- Generate result statistics
- Enrich source tables
- Create master tables
- Generate metadata

## Success Metrics

### Data Quality
- **Coverage**: % of records with canonical IDs
- **Uniqueness**: Distinct canonical IDs vs source IDs
- **Consistency**: Same canonical ID across tables

### Performance
- **Execution Time**: Minutes to hours depending on data volume
- **Iterations**: Typically 2-5 iterations for convergence
- **Throughput**: Millions of records per minute

### Output Tables
- **Lookup Table**: `{canonical_id}_lookup` - ID mappings
- **Enriched Tables**: `enriched_{table_name}` - Source + canonical_id
- **Master Tables**: `{master_name}` - Unified customer profiles
- **Statistics**: Source and result key statistics

## Best Practices

### 1. YAML Configuration
- Start with 2-3 key types, add more as needed
- Use regex validation for email, phone patterns
- Define invalid_texts to exclude test/dummy data
- Set merge_iterations conservatively (5-10)

### 2. SQL Generation
- Use source/target schema separation for safety
- Review generated SQL before execution
- Test with small data samples first

### 3. Execution
- Monitor convergence carefully
- Check statistics after each run
- Optimize Delta tables regularly (Databricks)
- Use appropriate warehouse sizing (Snowflake)

### 4. Troubleshooting
- **Slow convergence**: Increase merge_iterations or check data quality
- **High memory usage**: Reduce batch sizes or add clustering
- **Incorrect mappings**: Review key validation rules
- **Failed execution**: Check credentials and permissions

## Integration with Existing Workflows

### Treasure Data Migration
Convert existing TD unification to hybrid:
1. Export TD `unify.yml` configuration
2. Use `/cdp-hybrid-idu:hybrid-generate-databricks` or `hybrid-generate-snowflake`
3. Execute on target platform
4. Compare results for validation

### Incremental Processing
Add time-based filtering to YAML:
```yaml
tables:
  - table: customer_profiles
    filter: "WHERE updated_at > ${last_run_time}"
    key_columns:
      - {column: email, key: email}
```

### Master Table Sync
Schedule regular runs:
- Incremental: Process new/updated records
- Full refresh: Reprocess all data periodically

## Dependencies

### Python Requirements
```bash
# Databricks
pip install databricks-sql-connector pyyaml python-dotenv rich

# Snowflake
pip install snowflake-connector-python pyyaml python-dotenv rich
```

### Platform Requirements
- **Databricks**: Unity Catalog, SQL Warehouse or Cluster
- **Snowflake**: Database, Schema, Warehouse permissions

## Plugin Implementation Details

### Agent Coordination
```
User Request
    ↓
Main Command (hybrid-setup)
    ↓
yaml-configuration-builder Agent
    → Creates unify.yml
    ↓
Platform Selection
    ↓
databricks-sql-generator Agent          snowflake-sql-generator Agent
    → Calls Python script                  → Calls Python script
    → Generates Databricks SQL             → Generates Snowflake SQL
    ↓                                      ↓
databricks-workflow-executor Agent      snowflake-workflow-executor Agent
    → Calls Python executor                → Calls Python executor
    → Monitors convergence                 → Monitors convergence
    → Reports results                      → Reports results
```

### Python Script Integration
Agents use the Bash tool to execute Python scripts:
```bash
# Generation
python3 scripts/databricks/yaml_unification_to_databricks.py \
    unify.yml -tc catalog -ts schema -o output/

# Execution
python3 scripts/databricks/databricks_sql_executor.py \
    output/unify/ --server-hostname HOST --http-path PATH \
    --catalog CATALOG --schema SCHEMA --auth-type pat
```

## Support and Troubleshooting

### Common Issues
1. **Python not found**: Ensure Python 3.7+ is installed
2. **Module not found**: Install dependencies with pip
3. **Connection errors**: Verify credentials and network access
4. **SQL errors**: Review generated SQL for platform compatibility

### Getting Help
- Review generated SQL files for debugging
- Check execution logs in platform consoles
- Use `/cdp-hybrid-idu:hybrid-unif-config-validate` for configuration issues

## Summary

This plugin provides a **comprehensive, team-accessible solution** for multi-platform ID unification with:
- ✅ **Unified Interface**: Single YAML for multiple platforms
- ✅ **Production-Ready**: Battle-tested Python scripts with optimizations
- ✅ **Interactive**: Claude Code guided workflows
- ✅ **Intelligent**: Convergence detection and monitoring
- ✅ **Scalable**: Handles millions to billions of records
- ✅ **Team-Friendly**: Share configurations and workflows easily

**Ready to unify customer identities across Snowflake and Databricks!**
