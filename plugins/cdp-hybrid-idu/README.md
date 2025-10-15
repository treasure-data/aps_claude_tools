# CDP Hybrid IDU Plugin

**Cross-platform identity unification for Snowflake and Databricks with intelligent convergence detection**

---

## Overview

The CDP Hybrid IDU (Identity Unification) plugin enables customer identity resolution on Snowflake and Databricks platforms using YAML-driven configuration. It generates platform-native SQL with optimizations specific to each data warehouse and provides real-time execution monitoring with automatic convergence detection.

### Purpose

Unify customer identities across multiple data sources on modern cloud data platforms by automating:
- YAML configuration creation for ID unification rules
- Platform-native SQL generation (Snowflake/Databricks)
- Intelligent convergence loop execution
- Real-time monitoring and progress tracking
- Master customer record creation
- Enrichment of source tables with canonical IDs

---

## Supported Platforms

### Snowflake
- **SQL Dialect**: Snowflake SQL with VARIANT support
- **Authentication**: Password, SSO (externalbrowser), Key-Pair
- **Optimizations**: CLUSTER BY, ARRAY_CONSTRUCT, LATERAL FLATTEN
- **Execution**: Direct Snowflake connector (Python)

### Databricks
- **SQL Dialect**: Spark SQL with Delta Lake
- **Authentication**: Personal Access Token, OAuth
- **Optimizations**: Delta Lake ACID, COLLECT_LIST, EXPLODE
- **Execution**: Databricks SQL connector (Python)

---

## Features

### YAML-Driven Configuration
- **Single Source of Truth**: One `unify.yml` drives both platforms
- **Key Validation**: Regex patterns and invalid text filtering
- **Table Mapping**: Link tables to identity key types
- **Canonical IDs**: Define ID hierarchy and merge strategy
- **Master Tables**: Priority-based attribute selection

### Intelligent Convergence Detection
- **Automatic Loop**: Iterates until ID graph stabilizes
- **Change Detection**: Counts records updated per iteration
- **Early Termination**: Stops when updated_count = 0
- **Safety Limit**: Maximum iterations (default: 30)
- **Progress Monitoring**: Real-time iteration metrics

### Platform-Native SQL Generation
**Snowflake Features**:
- `ARRAY_CONSTRUCT()` for array creation
- `LATERAL FLATTEN()` for array unnesting
- `ARRAY_AGG() WITHIN GROUP (ORDER BY)` for ordered arrays
- `CLUSTER BY (follower_id)` for performance
- `VARIANT` data type for flexible structures
- `MAX_BY()` for priority-based selection

**Databricks Features**:
- `COLLECT_LIST()` for array aggregation
- `EXPLODE()` for array expansion
- `ARRAY()` constructor
- Delta Lake table format with ACID
- Spark SQL optimizations
- Window functions for deduplication

### Real-Time Execution
- **Connection Management**: Secure credential handling
- **Progress Display**: ✓, •, ✗ indicators
- **Row Counts**: Track records processed
- **Execution Times**: Monitor performance
- **Error Handling**: Interactive failure management

### Comprehensive Outputs
- **Graph Tables**: ID graph evolution per iteration
- **Canonical ID Lookup**: Final unified ID mapping
- **Enriched Tables**: Source tables with canonical IDs
- **Master Tables**: Unified customer profiles
- **Statistics**: Coverage and quality metrics
- **Metadata**: Lineage and column mapping

---

## Slash Commands

### `/cdp-hybrid-idu:hybrid-setup`

Complete end-to-end hybrid ID unification setup from scratch.

**Usage:**
```bash
/cdp-hybrid-idu:hybrid-setup
```

**Prompts for:**
- Platform (Snowflake or Databricks)
- Database and schema
- Tables for unification
- Identity keys (email, phone, customer_id, etc.)
- Master table attributes

**Executes:**
1. Creates `unify.yml` configuration
2. Validates YAML structure
3. Generates platform-specific SQL (20+ files)
4. (Optional) Executes workflow immediately

**Generates:**
- `unify.yml`
- `snowflake_sql/unify/*.sql` OR `databricks_sql/unify/*.sql`
- Execution report

---

### `/cdp-hybrid-idu:hybrid-unif-config-validate`

Validate YAML configuration before SQL generation.

**Usage:**
```bash
/cdp-hybrid-idu:hybrid-unif-config-validate
```

**Validates:**
- YAML syntax correctness
- Required sections (keys, tables, canonical_ids, master_tables)
- Key validation rules (regex, invalid_texts)
- Table and column mappings
- Canonical ID configuration
- Master table attribute priorities

**Output:**
```
Validating: unify.yml

✓ YAML syntax valid
✓ Required sections present:
  - keys (3 defined)
  - tables (3 mapped)
  - canonical_ids (1 defined)
  - master_tables (1 defined)
✓ Key validation rules complete
✓ Table mappings valid
✓ Canonical ID merge strategy defined
✓ Master table attributes configured

Overall: PASS
```

---

### `/cdp-hybrid-idu:hybrid-generate-snowflake`

Generate Snowflake SQL from YAML configuration.

**Usage:**
```bash
/cdp-hybrid-idu:hybrid-generate-snowflake
```

**Prompts for:**
- YAML file path (defaults to `unify.yml`)
- Target database
- Target schema
- Source database (optional, defaults to target)
- Source schema (optional, defaults to PUBLIC)
- Output directory (defaults to `snowflake_sql/`)

**Generates 20+ SQL Files:**

**Setup Phase:**
- `01_create_graph.sql` - Initialize graph table
- `02_extract_merge.sql` - Extract identities from sources
- `03_source_key_stats.sql` - Source statistics

**Loop Phase:**
- `04_unify_loop_iteration_01.sql` through `04_unify_loop_iteration_N.sql`
  - One file per iteration (N from YAML config)
  - Convergence detection built-in

**Canonicalization:**
- `05_canonicalize.sql` - Create canonical ID lookup
- `06_result_key_stats.sql` - Result statistics

**Enrichment:**
- `10_enrich_{table_name}.sql` - One per source table
  - Adds canonical_id column to original tables

**Master Tables:**
- `20_master_{master_table_name}.sql` - Unified customer profiles

**Metadata:**
- `30_unification_metadata.sql` - Process metadata
- `31_filter_lookup.sql` - Validation rules lookup
- `32_column_lookup.sql` - Column mappings lookup

**Snowflake-Specific Conversions:**
```
ARRAY() → ARRAY_CONSTRUCT()
COLLECT_LIST() → ARRAY_AGG()
EXPLODE() → LATERAL FLATTEN()
BOOL_OR() → BOOLOR_AGG()
SIZE() → ARRAY_SIZE()
```

---

### `/cdp-hybrid-idu:hybrid-execute-snowflake`

Execute Snowflake SQL workflow with convergence detection.

**Usage:**
```bash
/cdp-hybrid-idu:hybrid-execute-snowflake
```

**Prompts for:**
- SQL directory (defaults to `snowflake_sql/unify/`)
- Snowflake account
- Username
- Database and schema
- Warehouse name
- Authentication (password/SSO/key-pair)

**Execution Flow:**
1. **Connect** to Snowflake with credentials
2. **Validate** SQL files and dependencies
3. **Execute Setup** (01-03 files)
4. **Run Convergence Loop**:
   - Execute iteration 1
   - Check convergence (count updates)
   - If updates > 0, continue to next iteration
   - If updates = 0, stop (converged)
   - Create `loop_final` alias table
5. **Post-Loop Processing** (05-06)
6. **Enrich Tables** (10-19)
7. **Create Master Tables** (20-29)
8. **Generate Metadata** (30-39)

**Real-Time Output:**
```
✓ Connected to Snowflake: RQRABIH-TREASUREDATA_PARTNER_BIZCRIT
• Using database: INDRESH_TEST, schema: PUBLIC

Executing: 01_create_graph.sql
✓ 01_create_graph.sql: Executed successfully
• Rows affected: 1

Executing: 02_extract_merge.sql
✓ 02_extract_merge.sql: Executed successfully
• Rows affected: 14,573

Executing Unify Loop Before Canonicalization

--- Iteration 1 ---
✓ Iteration 1 completed
• Rows processed: 13,045
• Updated records: 1,565

--- Iteration 2 ---
✓ Iteration 2 completed
• Rows processed: 13,035
• Updated records: 15

--- Iteration 3 ---
✓ Iteration 3 completed
• Rows processed: 13,034
• Updated records: 1

--- Iteration 4 ---
✓ Iteration 4 completed
• Rows processed: 13,034
• Updated records: 0
✓ Loop converged after 4 iterations
```

**Final Report:**
- Total execution time
- Iterations to convergence
- Final canonical ID count
- Table row counts
- Coverage metrics

---

### `/cdp-hybrid-idu:hybrid-generate-databricks`

Generate Databricks Delta Lake SQL from YAML configuration.

**Usage:**
```bash
/cdp-hybrid-idu:hybrid-generate-databricks
```

**Prompts for:**
- YAML file path
- Catalog name (Unity Catalog)
- Schema name
- Output directory (defaults to `databricks_sql/`)

**Generates:**
Similar structure to Snowflake but with Databricks-specific SQL:

**Databricks-Specific Conversions:**
```
ARRAY_CONSTRUCT() → ARRAY()
LATERAL FLATTEN() → EXPLODE()
ARRAY_AGG() → COLLECT_LIST()
CLUSTER BY → OPTIMIZE ... ZORDER BY
VARIANT → STRUCT/MAP types
```

**Delta Lake Features:**
- ACID transactions
- Time travel support
- Schema evolution
- OPTIMIZE and ZORDER

---

### `/cdp-hybrid-idu:hybrid-execute-databricks`

Execute Databricks SQL workflow with convergence detection.

**Usage:**
```bash
/cdp-hybrid-idu:hybrid-execute-databricks
```

**Prompts for:**
- SQL directory
- Databricks workspace URL
- Personal Access Token
- Catalog and schema
- Warehouse/cluster ID

**Execution:**
Same convergence loop logic as Snowflake, adapted for Databricks SQL connector.

---

## YAML Configuration Reference

### Complete unify.yml Example

```yaml
name: customer_unification

#####################################################
## Validation logic for unification keys
#####################################################
keys:
  - name: email
    valid_regexp: ".*@.*"
    invalid_texts: ['', 'N/A', 'null', 'test@example.com']

  - name: customer_id
    invalid_texts: ['', 'N/A', 'null', '0']

  - name: phone_number
    valid_regexp: "^[0-9]{10,}$"
    invalid_texts: ['', 'N/A', 'null', '0000000000']

#####################################################
## Databases, tables, and keys for unification
#####################################################
tables:
  - database: customer_db
    table: customer_profiles_staging
    key_columns:
      - {column: email_std, key: email}
      - {column: customer_id, key: customer_id}

  - database: customer_db
    table: customer_transactions
    key_columns:
      - {column: customer_email, key: email}
      - {column: phone, key: phone_number}

  - database: customer_db
    table: web_sessions
    key_columns:
      - {column: user_email, key: email}

#####################################################
## Canonical ID hierarchy and merge strategy
#####################################################
canonical_ids:
  - name: unified_customer_id
    merge_by_keys: [email, customer_id, phone_number]
    # Optional: key_priorities: [3, 1, 2]
    merge_iterations: 15

#####################################################
## Master table attributes and aggregation rules
#####################################################
master_tables:
  - name: customer_master
    canonical_id: unified_customer_id
    attributes:
      # Single value with priority
      - name: primary_email
        source_columns:
          - {table: customer_profiles_staging, column: email_std, order: last, order_by: time, priority: 1}
          - {table: customer_transactions, column: customer_email, order: last, order_by: time, priority: 2}

      # Array of values (top N)
      - name: all_emails
        array_elements: 5
        source_columns:
          - {table: customer_profiles_staging, column: email_std, order: last, order_by: time, priority: 1}
          - {table: customer_transactions, column: customer_email, order: last, order_by: time, priority: 2}
          - {table: web_sessions, column: user_email, order: last, order_by: time, priority: 3}

      # Best phone number
      - name: phone
        source_columns:
          - {table: customer_transactions, column: phone, order: last, order_by: time, priority: 1}

      # Customer ID
      - name: customer_id
        source_columns:
          - {table: customer_profiles_staging, column: customer_id, order: last, order_by: time, priority: 1}
```

### YAML Sections Explained

#### Keys Section
Define identity types and validation rules:
```yaml
keys:
  - name: email              # Key type name
    valid_regexp: ".*@.*"    # Validation pattern (optional)
    invalid_texts:           # Values to exclude
      - ''
      - 'N/A'
      - 'null'
```

#### Tables Section
Map source tables to key columns:
```yaml
tables:
  - database: my_db          # Database name
    table: my_table          # Table name
    key_columns:             # Columns containing keys
      - column: email_field  # Actual column name
        key: email           # Maps to key type
```

#### Canonical IDs Section
Define merge strategy:
```yaml
canonical_ids:
  - name: unified_id         # Canonical ID name
    merge_by_keys:           # Keys to merge on
      - email
      - phone
    merge_iterations: 15     # Max iterations
```

#### Master Tables Section
Define unified customer profile:
```yaml
master_tables:
  - name: master_customers
    canonical_id: unified_id
    attributes:
      - name: best_email                      # Attribute name
        source_columns:
          - table: source_table               # Source table
            column: email                     # Source column
            order: last                       # first/last
            order_by: time                    # Order by column
            priority: 1                       # Priority (1=highest)

      - name: all_phones                      # Array attribute
        array_elements: 3                     # Top N elements
        source_columns:
          - {table: t1, column: phone, priority: 1}
          - {table: t2, column: mobile, priority: 2}
```

---

## Convergence Algorithm

### How It Works

```python
iteration = 1
max_iterations = 10
converged = False

while iteration <= max_iterations and not converged:
    # Execute unification iteration SQL
    execute(f"04_unify_loop_iteration_{iteration:02d}.sql")

    # Check convergence
    updated_count = query(f"""
        SELECT COUNT(*) FROM (
            SELECT leader_ns, leader_id, follower_ns, follower_id
            FROM graph_loop_{iteration}
            EXCEPT
            SELECT leader_ns, leader_id, follower_ns, follower_id
            FROM graph_loop_{iteration - 1}
        ) diff
    """)

    if updated_count == 0:
        converged = True
        print(f"✓ Converged after {iteration} iterations")
    else:
        print(f"• Iteration {iteration}: {updated_count} records updated")
        iteration += 1

# Create alias for downstream processing
execute(f"""
    CREATE TABLE graph_loop_final AS
    SELECT * FROM graph_loop_{iteration}
""")
```

### Example Convergence

**Iteration 1:**
- Input: 14,573 edges from 3 source tables
- Output: 13,045 edges (1,565 IDs merged)
- Continue → updated_count > 0

**Iteration 2:**
- Input: 13,045 edges
- Output: 13,035 edges (15 IDs merged)
- Continue → updated_count > 0

**Iteration 3:**
- Input: 13,035 edges
- Output: 13,034 edges (1 ID merged)
- Continue → updated_count > 0

**Iteration 4:**
- Input: 13,034 edges
- Output: 13,034 edges (0 IDs merged)
- **STOP** → Converged!

**Result:** 13,033 canonical IDs created in 4 iterations (60% faster than max 10)

---

## Usage Examples

### Example 1: Snowflake End-to-End

```bash
# Generate SQL from YAML
/cdp-hybrid-idu:hybrid-generate-snowflake

# Inputs:
# - YAML: unify.yml
# - Database: CUSTOMER_DB
# - Schema: PUBLIC
# - Output: snowflake_sql/

# Execute workflow
/cdp-hybrid-idu:hybrid-execute-snowflake

# Inputs:
# - SQL dir: snowflake_sql/unify/
# - Account: myorg-myaccount
# - User: analyst_user
# - Database: CUSTOMER_DB
# - Schema: PUBLIC
# - Warehouse: COMPUTE_WH
# - Password: (from .env or prompt)

# Result:
# - 4 iterations to convergence
# - 13,033 canonical IDs
# - Enriched 3 source tables
# - Created customer_master table
```

### Example 2: Databricks Delta Lake

```bash
# Generate Databricks SQL
/cdp-hybrid-idu:hybrid-generate-databricks

# Inputs:
# - YAML: unify.yml
# - Catalog: customer_catalog
# - Schema: unified
# - Output: databricks_sql/

# Execute
/cdp-hybrid-idu:hybrid-execute-databricks

# Inputs:
# - Workspace: https://myorg.databricks.com
# - Token: dapi...
# - Catalog: customer_catalog
# - Schema: unified
# - Warehouse: SQL Warehouse ID
```

### Example 3: Validation Before Generation

```bash
# Validate YAML first
/cdp-hybrid-idu:hybrid-unif-config-validate

# If PASS, generate SQL
/cdp-hybrid-idu:hybrid-generate-snowflake

# Execute
/cdp-hybrid-idu:hybrid-execute-snowflake
```

---

## Authentication

### Snowflake Authentication Options

**Option 1: Password**
```bash
# Via environment variable
export SNOWFLAKE_PASSWORD='your_password'

# Or via .env file
echo "SNOWFLAKE_PASSWORD=your_password" > .env

# Or prompt during execution
/cdp-hybrid-idu:hybrid-execute-snowflake
# Will prompt: Enter password:
```

**Option 2: SSO (externalbrowser)**
```bash
/cdp-hybrid-idu:hybrid-execute-snowflake
# Choose: SSO authentication
# Opens browser for authentication
```

**Option 3: Key-Pair**
```bash
export SNOWFLAKE_PRIVATE_KEY_PATH='/path/to/key.p8'
export SNOWFLAKE_PRIVATE_KEY_PASSPHRASE='passphrase'

/cdp-hybrid-idu:hybrid-execute-snowflake
```

### Databricks Authentication

**Personal Access Token:**
```bash
export DATABRICKS_TOKEN='dapi...'

/cdp-hybrid-idu:hybrid-execute-databricks
```

---

## Best Practices

### 1. YAML Configuration
- Start with 2-3 key types (email, customer_id)
- Add more keys incrementally
- Test validation rules with sample data
- Use realistic invalid_texts list

### 2. Iteration Count
- Set merge_iterations to 10-15 initially
- Monitor actual iterations to convergence
- Adjust based on data complexity
- Most datasets converge in 3-7 iterations

### 3. Master Table Design
- Use priority 1 for most reliable sources
- Create both single-value and array attributes
- Include order_by timestamp for recency
- Test attribute selection logic

### 4. Performance
- Use appropriate warehouse size (Snowflake)
- Enable auto-scaling for large datasets
- Monitor execution times per iteration
- Use CLUSTER BY on large tables

### 5. Data Quality
- Clean data before unification
- Standardize formats (lowercase emails)
- Filter test/invalid data
- Validate key quality metrics

---

## Monitoring and Validation

### Check Convergence Progress

```sql
-- Snowflake
SELECT iteration, COUNT(*) as edge_count
FROM (
    SELECT 1 as iteration, COUNT(*) FROM unified_id_graph_unify_loop_1
    UNION ALL
    SELECT 2, COUNT(*) FROM unified_id_graph_unify_loop_2
    UNION ALL
    SELECT 3, COUNT(*) FROM unified_id_graph_unify_loop_3
    UNION ALL
    SELECT 4, COUNT(*) FROM unified_id_graph_unify_loop_4
)
ORDER BY iteration;
```

### Verify Canonical ID Coverage

```sql
SELECT
    'customer_profiles' AS table_name,
    COUNT(*) AS total_records,
    COUNT(unified_customer_id) AS with_canonical_id,
    ROUND(100.0 * COUNT(unified_customer_id) / COUNT(*), 2) AS coverage_pct
FROM customer_db.customer_profiles_staging

UNION ALL

SELECT
    'customer_transactions',
    COUNT(*),
    COUNT(unified_customer_id),
    ROUND(100.0 * COUNT(unified_customer_id) / COUNT(*), 2)
FROM customer_db.customer_transactions;
```

### Review Master Table

```sql
SELECT * FROM customer_db.customer_master LIMIT 10;

-- Check attribute completeness
SELECT
    COUNT(*) AS total_customers,
    COUNT(primary_email) AS with_email,
    COUNT(phone) AS with_phone,
    ARRAY_SIZE(all_emails) AS email_array_avg
FROM customer_db.customer_master;
```

---

## Troubleshooting

### Issue: Loop Not Converging

**Symptoms:** Iterations reach max without convergence

**Solutions:**
- Increase merge_iterations in YAML
- Check data quality (duplicates, invalid keys)
- Review validation rules (too strict?)
- Examine graph structure for cycles

### Issue: Too Few Canonical IDs

**Symptoms:** Most records map to one ID

**Solutions:**
- Check key validation (invalid_texts too broad?)
- Verify data quality in source tables
- Review merge_by_keys (too many keys?)
- Check for data standardization issues

### Issue: SQL Execution Failure

**Symptoms:** SQL file fails to execute

**Solutions:**
- Verify table names and schemas
- Check column existence in source tables
- Validate data types
- Review generated SQL for syntax errors

### Issue: Authentication Failure

**Solutions:**
- Verify credentials (password, token)
- Check account/workspace URL format
- Ensure warehouse/cluster is running
- Validate permissions on database/schema

---

## File Structure

```
hybrid-idu-project/
├── unify.yml                          # Configuration
├── .env                               # Credentials (gitignored)
│
├── snowflake_sql/
│   └── unify/
│       ├── 01_create_graph.sql
│       ├── 02_extract_merge.sql
│       ├── 03_source_key_stats.sql
│       ├── 04_unify_loop_iteration_01.sql
│       ├── ...
│       ├── 04_unify_loop_iteration_10.sql
│       ├── 05_canonicalize.sql
│       ├── 06_result_key_stats.sql
│       ├── 10_enrich_*.sql
│       ├── 20_master_*.sql
│       ├── 30_unification_metadata.sql
│       ├── 31_filter_lookup.sql
│       └── 32_column_lookup.sql
│
└── databricks_sql/
    └── unify/
        └── (same structure as Snowflake)
```

---

## Python Scripts

### Snowflake Scripts

**Location:** `plugins/cdp-hybrid-idu/scripts/snowflake/`

**yaml_unification_to_snowflake.py:**
- Reads unify.yml
- Converts to Snowflake SQL
- Applies Snowflake-specific syntax
- Generates all SQL files

**snowflake_sql_executor.py:**
- Connects to Snowflake
- Executes SQL files in order
- Implements convergence detection
- Provides real-time monitoring

### Databricks Scripts

**Location:** `plugins/cdp-hybrid-idu/scripts/databricks/`

**yaml_unification_to_databricks.py:**
- Reads unify.yml
- Converts to Spark SQL
- Applies Delta Lake optimizations
- Generates all SQL files

**databricks_sql_executor.py:**
- Connects to Databricks SQL
- Executes workflows
- Monitors convergence
- Tracks execution metrics

---

## Quality Gates

All hybrid IDU implementations must include:

1. **YAML Validation**: Syntax and structure correct
2. **Key Validation**: Regex and invalid_texts defined
3. **Table Mapping**: All source tables mapped to keys
4. **Canonical ID Config**: Merge strategy defined
5. **Master Table**: Attributes with priorities
6. **Convergence Logic**: Built into loop iterations
7. **Statistics**: Source and result metrics
8. **Metadata**: Complete lineage tracking

---

## Performance Benchmarks

### Typical Performance (Snowflake)

| Dataset Size | Tables | Keys | Iterations | Time | Canonical IDs |
|--------------|--------|------|------------|------|---------------|
| 100K records | 3 | 3 | 4 | 2 min | ~85K |
| 1M records | 5 | 3 | 5 | 8 min | ~750K |
| 10M records | 8 | 4 | 6 | 25 min | ~7.5M |

**Warehouse:** Medium (Snowflake)

### Optimization Tips

- Use larger warehouse for datasets > 5M records
- Enable multi-cluster for concurrency
- Add CLUSTER BY on frequently joined columns
- Monitor query profile for bottlenecks

---

## Support

For assistance:
- Review generated SQL in output directory
- Check validation results
- Examine execution logs
- Verify source data quality

---

**Version:** 1.5.0
**Last Updated:** 2025-10-13
**Maintained by:** APS CDP Team
