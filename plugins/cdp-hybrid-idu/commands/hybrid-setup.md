---
name: hybrid-setup
description: Complete end-to-end hybrid ID unification setup - automatically analyzes tables, generates config, creates SQL, and executes workflow for Snowflake and Databricks
---

# Hybrid ID Unification Complete Setup

## Overview

I'll guide you through the complete hybrid ID unification setup process for Snowflake and/or Databricks platforms. This is an **automated, end-to-end workflow** that will:

1. **Analyze your tables automatically** using platform MCP tools with strict PII detection
2. **Generate YAML configuration** from real schema and data analysis
3. **Choose target platform(s)** (Snowflake, Databricks, or both)
4. **Generate platform-specific SQL** optimized for each engine
5. **Execute workflows** with convergence detection and monitoring
6. **Provide deployment guidance** and operating instructions

**Key Features**:
- 🔍 **Automated Table Analysis**: Uses Snowflake/Databricks MCP tools to analyze actual tables
- ✅ **Strict PII Detection**: Zero tolerance - only includes tables with real user identifiers
- 📊 **Real Data Validation**: Queries actual data to validate patterns and quality
- 🎯 **Smart Recommendations**: Expert analysis provides merge strategy and priorities
- 🚀 **End-to-End Automation**: From table analysis to workflow execution

---

## What You Need to Provide

### 1. Unification Requirements (For Automated Analysis)
- **Platform**: Snowflake or Databricks
- **Tables**: List of source tables to analyze
  - Format (Snowflake): `database.schema.table` or `schema.table` or `table`
  - Format (Databricks): `catalog.schema.table` or `schema.table` or `table`
- **Canonical ID Name**: Name for your unified ID (e.g., `td_id`, `unified_customer_id`)
- **Merge Iterations**: Number of unification loops (default: 10)
- **Master Tables**: (Optional) Attribute aggregation specifications

**Note**: The system will automatically:
- Extract user identifiers from actual table schemas
- Validate data patterns from real data
- Apply appropriate validation rules based on data analysis
- Generate merge strategy recommendations

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

### Step 1: Automated YAML Configuration Generation
I'll use the **hybrid-unif-config-creator** command to automatically generate your `unify.yml` file:

**Automated Analysis Approach** (Recommended):
- Analyze your actual tables using platform MCP tools (Snowflake/Databricks)
- Extract user identifiers with STRICT PII detection (zero tolerance for guessing)
- Validate data patterns from real table data
- Generate unify.yml with exact template compliance
- Only include tables with actual user identifiers
- Document excluded tables with detailed reasons

**What I'll do**:
- Call the **hybrid-unif-keys-extractor agent** to analyze tables
- Query actual schema and data using platform MCP tools
- Detect valid user identifiers (email, customer_id, phone, etc.)
- Exclude tables without PII with full documentation
- Generate production-ready unify.yml automatically

**Alternative - Manual Configuration**:
- If MCP tools are unavailable, I'll guide you through manual configuration
- Interactive prompts for keys, tables, and validation rules
- Step-by-step YAML building with validation

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

This command orchestrates the complete end-to-end flow by calling specialized commands in sequence:

### Phase 1: Configuration Creation
**I'll ask you for**:
- Platform (Snowflake or Databricks)
- Tables to analyze
- Canonical ID name
- Merge iterations

**Then I'll**:
- Call `/cdp-hybrid-idu:hybrid-unif-config-creator` internally
- Analyze your tables automatically
- Generate `unify.yml` with strict PII detection
- Show you the configuration for review

### Phase 2: SQL Generation
**I'll ask you**:
- Which platform(s) to generate SQL for (can be different from source)
- Output directory preferences

**Then I'll**:
- Call `/cdp-hybrid-idu:hybrid-generate-snowflake` (if Snowflake selected)
- Call `/cdp-hybrid-idu:hybrid-generate-databricks` (if Databricks selected)
- Generate 15+ optimized SQL files per platform
- Show you the execution plan

### Phase 3: Workflow Execution (Optional)
**I'll ask you**:
- Do you want to execute now or later?
- Connection credentials if executing

**Then I'll**:
- Call `/cdp-hybrid-idu:hybrid-execute-snowflake` (if Snowflake selected)
- Call `/cdp-hybrid-idu:hybrid-execute-databricks` (if Databricks selected)
- Monitor convergence and progress
- Report final statistics

**Throughout the process**:
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

**Ready to begin?** I'll use the **hybrid-unif-config-creator** to automatically analyze your tables and generate the YAML configuration.

Please provide:

1. **Platform**: Which platform contains your data?
   - Snowflake or Databricks

2. **Tables**: Which source tables should I analyze?
   - Format (Snowflake): `database.schema.table` or `schema.table` or `table`
   - Format (Databricks): `catalog.schema.table` or `schema.table` or `table`
   - Example: `customer_db.public.customers`, `orders`, `web_events.user_activity`

3. **Canonical ID Name**: What should I call the unified ID?
   - Example: `td_id`, `unified_customer_id`, `master_id`
   - Default: `td_id`

4. **Merge Iterations** (optional): How many unification loops?
   - Default: 10
   - Range: 2-30

5. **Target Platform(s)** for SQL generation:
   - Same as source, or generate for both platforms

**Example**:
```
I want to set up hybrid ID unification for:

Platform: Snowflake
Tables:
- customer_db.public.customer_profiles
- customer_db.public.orders
- marketing_db.public.campaigns
- event_db.public.web_events

Canonical ID: unified_customer_id
Merge Iterations: 10
Generate SQL for: Snowflake (or both Snowflake and Databricks)
```

**What I'll do next**:
1. ✅ Analyze your tables using Snowflake MCP tools
2. ✅ Extract user identifiers with strict PII detection
3. ✅ Generate unify.yml automatically
4. ✅ Generate platform-specific SQL files
5. ✅ Execute workflow (if requested)
6. ✅ Provide deployment guidance

---

**Let's get started with your hybrid ID unification setup!**
