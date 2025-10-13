---
name: hybrid-setup
description: Complete end-to-end hybrid ID unification setup for Snowflake and Databricks platforms
---

# Hybrid ID Unification Complete Setup

## Overview

I'll guide you through the complete hybrid ID unification setup process for Snowflake and/or Databricks platforms. This is an interactive, end-to-end workflow that will:

1. **Create YAML configuration** interactively with validation
2. **Choose target platform(s)** (Snowflake, Databricks, or both)
3. **Generate platform-specific SQL** optimized for each engine
4. **Execute workflows** with convergence detection and monitoring
5. **Provide deployment guidance** and operating instructions

---

## What You Need to Provide

### 1. Unification Requirements
- **Tables**: List of source tables for unification
- **Keys**: User identifier columns (email, phone, customer_id, etc.)
- **Validation Rules**: Regex patterns and invalid values (optional)
- **Master Tables**: Attribute aggregation configurations (optional)

### 2. Platform Selection
- **Databricks**: Unity Catalog with Delta Lake
- **Snowflake**: Database with proper permissions
- **Both**: Generate SQL for both platforms

### 3. Target Configurations

**For Databricks**:
- **Catalog**: Target catalog name
- **Schema**: Target schema name
- **Source Catalog** (optional): Source data catalog
- **Source Schema** (optional): Source data schema

**For Snowflake**:
- **Database**: Target database name
- **Schema**: Target schema name
- **Source Schema** (optional): Source data schema

### 4. Execution Credentials (if executing)

**For Databricks**:
- **Server Hostname**: your-workspace.databricks.com
- **HTTP Path**: /sql/1.0/warehouses/your-warehouse-id
- **Authentication**: PAT (Personal Access Token) or OAuth

**For Snowflake**:
- **Account**: Snowflake account name
- **User**: Username
- **Password**: Password or use SSO/key-pair
- **Warehouse**: Compute warehouse name

---

## What I'll Do

### Step 1: Interactive YAML Configuration Builder
I'll call the **yaml-configuration-builder agent** to help you create a proper `unify.yml` file:
- Define keys with validation rules
- Map tables to key columns
- Configure canonical ID merge strategies
- Set up master tables (optional)
- Validate configuration syntax

### Step 2: Platform Selection and Configuration
I'll help you:
- Choose between Databricks, Snowflake, or both
- Collect platform-specific configuration (catalog/database, schema names)
- Determine source/target separation strategy
- Decide on execution or generation-only mode

### Step 3: SQL Generation

**For Databricks** (if selected):
I'll call the **databricks-sql-generator agent** to:
- Execute `yaml_unification_to_databricks.py` script
- Generate Delta Lake optimized SQL workflow
- Create output directory: `databricks_sql/unify/`
- Generate 15+ SQL files with proper execution order

**For Snowflake** (if selected):
I'll call the **snowflake-sql-generator agent** to:
- Execute `yaml_unification_to_snowflake.py` script
- Generate Snowflake-native SQL workflow
- Create output directory: `snowflake_sql/unify/`
- Generate 15+ SQL files with proper execution order

### Step 4: Workflow Execution (Optional)

**For Databricks** (if execution requested):
I'll call the **databricks-workflow-executor agent** to:
- Execute `databricks_sql_executor.py` script
- Connect to your Databricks workspace
- Run SQL files in proper sequence
- Monitor convergence and progress
- Optimize Delta tables
- Report final statistics

**For Snowflake** (if execution requested):
I'll call the **snowflake-workflow-executor agent** to:
- Execute `snowflake_sql_executor.py` script
- Connect to your Snowflake account
- Run SQL files in proper sequence
- Monitor convergence and progress
- Report final statistics

### Step 5: Deployment Guidance
I'll provide:
- Configuration summary
- Generated files overview
- Deployment instructions
- Operating guidelines
- Monitoring recommendations

---

## Interactive Workflow

I'll guide you through each step with:
- **Questions**: When I need your input
- **Suggestions**: Recommended approaches based on best practices
- **Validation**: Real-time checks on your choices
- **Explanations**: Help you understand concepts and options

---

## Expected Output

### Files Created (Platform-specific):

**For Databricks**:
```
databricks_sql/unify/
├── 01_create_graph.sql              # Initialize identity graph
├── 02_extract_merge.sql             # Extract and merge identities
├── 03_source_key_stats.sql          # Source statistics
├── 04_unify_loop_iteration_*.sql    # Iterative unification (N files)
├── 05_canonicalize.sql              # Canonical ID creation
├── 06_result_key_stats.sql          # Result statistics
├── 10_enrich_*.sql                  # Source table enrichment (N files)
├── 20_master_*.sql                  # Master table creation (N files)
├── 30_unification_metadata.sql      # Metadata tables
├── 31_filter_lookup.sql             # Validation rules
└── 32_column_lookup.sql             # Column mappings
```

**For Snowflake**:
```
snowflake_sql/unify/
├── 01_create_graph.sql              # Initialize identity graph
├── 02_extract_merge.sql             # Extract and merge identities
├── 03_source_key_stats.sql          # Source statistics
├── 04_unify_loop_iteration_*.sql    # Iterative unification (N files)
├── 05_canonicalize.sql              # Canonical ID creation
├── 06_result_key_stats.sql          # Result statistics
├── 10_enrich_*.sql                  # Source table enrichment (N files)
├── 20_master_*.sql                  # Master table creation (N files)
├── 30_unification_metadata.sql      # Metadata tables
├── 31_filter_lookup.sql             # Validation rules
└── 32_column_lookup.sql             # Column mappings
```

**Configuration**:
```
unify.yml                            # YAML configuration (created interactively)
```

---

## Success Criteria

All generated files will:
- ✅ Be platform-optimized and production-ready
- ✅ Use proper SQL dialects (Databricks Spark SQL or Snowflake SQL)
- ✅ Include convergence detection logic
- ✅ Support incremental processing
- ✅ Generate comprehensive statistics
- ✅ Work without modification on target platforms

---

## Getting Started

**Ready to begin?** I'll start by helping you create the YAML configuration.

Please provide:
1. **Tables**: Which source tables do you want to unify?
   - Format: `table_name` (e.g., `customer_profiles`, `orders`, `web_events`)

2. **Keys**: What user identifier columns do you have?
   - Common keys: email, customer_id, phone_number, td_client_id, user_id

3. **Platform**: Which platform(s) are you targeting?
   - Databricks, Snowflake, or both

**Example**:
```
I want to set up hybrid ID unification for:

Tables:
- customer_profiles
- orders
- web_events

Keys:
- email
- customer_id
- phone_number

Platform: Databricks (or Snowflake, or both)
```

---

**Let's get started with your hybrid ID unification setup!**
