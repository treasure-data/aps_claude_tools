# APS CDP Tools Marketplace

**Production-ready Claude Code plugins for Customer Data Platform (CDP) implementation automation**

---

## Overview

The APS CDP Tools Marketplace is a collection of Claude Code plugins designed to automate the entire CDP implementation lifecycle in Treasure Data. These plugins enforce production-tested patterns, strict quality gates, and comprehensive validation to ensure reliable, maintainable data pipelines.

### Mission

Accelerate CDP implementation from weeks to days by providing AI-powered, template-driven automation for:
- Data ingestion from multiple sources
- Data transformation and quality improvement
- Historical and incremental data consolidation
- Customer identity unification

---

## Architecture

### Plugin-Based Marketplace

The marketplace follows a modular architecture where each plugin handles a specific phase of the CDP pipeline:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        APS CDP Tools Marketplace                        │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
        ┌───────────────────────────┼───────────────────────────┐
        │                           │                           │
        ▼                           ▼                           ▼
┌───────────────┐          ┌────────────────┐         ┌────────────────┐
│ cdp-ingestion │          │ cdp-histunion  │         │  cdp-staging   │
│               │          │                │         │                │
│ Data Sources  │  ───────▶│ Consolidation  │ ───────▶│ Transformation │
│ • BigQuery    │          │ • Hist + Inc   │         │ • Cleansing    │
│ • Klaviyo     │          │ • Watermarks   │         │ • PII Handling │
│ • Shopify     │          │ • Schema Mgmt  │         │ • Validation   │
│ • OneTrust    │          │                │         │ • JSON Extract │
│ • SFTP        │          │                │         │                │
└───────────────┘          └────────────────┘         └────────────────┘
        │                           │                           │
        └───────────────────────────┼───────────────────────────┘
                                    ▼
                 ┌──────────────────────────────────────┐
                 │       Identity Unification           │
                 └──────────────────────────────────────┘
                           │                 │
              ┌────────────┴────────┬────────┴──────────┐
              ▼                     ▼                    ▼
    ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
    │ cdp-unification  │  │ cdp-hybrid-idu   │  │ cdp-hybrid-idu   │
    │                  │  │                  │  │                  │
    │ Treasure Data    │  │   Snowflake      │  │   Databricks     │
    │ • ID Resolution  │  │ • Auto YAML Gen  │  │ • Auto YAML Gen  │
    │ • Entity Merge   │  │ • IDU SQL Gen    │  │ • IDU SQL Gen    │
    │ • Master Records │  │ • IDU Execution  │  │ • IDU Execution  │
    │ • TD Workflow    │  │ • Merge Reports  │  │ • Merge Reports  │
    └──────────────────┘  └──────────────────┘  └──────────────────┘
```

### Plugin Directory Structure

```
aps_claude_tools/
├── .claude-plugin/
│   └── marketplace.json                 # Marketplace registry
├── plugins/
│   ├── cdp-ingestion/                   # Phase 1: Data Ingestion
│   │   ├── plugin.json
│   │   ├── prompt.md
│   │   ├── agents/
│   │   │   └── cdp-ingestion-expert.md
│   │   ├── commands/
│   │   │   ├── ingest-new.md
│   │   │   ├── ingest-add-klaviyo.md
│   │   │   ├── ingest-add-object.md
│   │   │   └── ingest-validate-wf.md
│   │   └── docs/
│   │       ├── patterns/
│   │       └── sources/
│   │
│   ├── cdp-histunion/                   # Phase 2: Data Consolidation
│   │   ├── plugin.json
│   │   ├── prompt.md
│   │   ├── agents/
│   │   │   └── cdp-histunion-expert.md
│   │   ├── commands/
│   │   │   ├── histunion-create.md
│   │   │   ├── histunion-batch.md
│   │   │   └── histunion-validate.md
│   │   └── docs/
│   │       └── examples.md
│   │
│   ├── cdp-staging/                     # Phase 3: Data Transformation
│   │   ├── plugin.json
│   │   ├── prompt.md
│   │   ├── agents/
│   │   │   ├── staging-transformer-presto.md
│   │   │   └── staging-transformer-hive.md
│   │   ├── commands/
│   │   │   ├── transform-table.md
│   │   │   ├── transform-batch.md
│   │   │   └── transform-validation.md
│   │   └── docs/
│   │
│   ├── cdp-unification/                 # Phase 4a: Identity Resolution (TD)
│   │   ├── plugin.json
│   │   ├── prompt.md
│   │   ├── agents/
│   │   │   └── cdp-unification-expert.md
│   │   ├── commands/
│   │   │   ├── unify-setup.md
│   │   │   ├── unify-extract-keys.md
│   │   │   ├── unify-create-prep.md
│   │   │   └── unify-create-config.md
│   │   └── docs/
│   │
│   └── cdp-hybrid-idu/                  # Phase 4b: Hybrid ID Unification
│       ├── plugin.json
│       ├── prompt.md
│       ├── agents/
│       │   ├── yaml-configuration-builder.md
│       │   ├── hybrid-unif-keys-extractor.md
│       │   ├── snowflake-sql-generator.md
│       │   ├── snowflake-workflow-executor.md
│       │   ├── databricks-sql-generator.md
│       │   ├── databricks-workflow-executor.md
│       │   └── merge-stats-report-generator.md
│       ├── commands/
│       │   ├── hybrid-setup.md
│       │   ├── hybrid-unif-config-creator.md
│       │   ├── hybrid-unif-config-validate.md
│       │   ├── hybrid-generate-snowflake.md
│       │   ├── hybrid-execute-snowflake.md
│       │   ├── hybrid-generate-databricks.md
│       │   ├── hybrid-execute-databricks.md
│       │   └── hybrid-unif-merge-stats-creator.md
│       ├── scripts/
│       │   ├── snowflake/
│       │   │   ├── yaml_unification_to_snowflake.py
│       │   │   └── snowflake_sql_executor.py
│       │   └── databricks/
│       │       ├── yaml_unification_to_databricks.py
│       │       └── databricks_sql_executor.py
│       └── docs/
│
└── README.md                            # This file
```

---

## Plugins Overview

### 1. CDP Ingestion (`cdp-ingestion`)

**Purpose**: Automate data ingestion from various sources into Treasure Data raw layer.

**Key Features**:
- Template-driven workflow generation
- Support for 6+ data sources (BigQuery, Klaviyo, Shopify, OneTrust, Pinterest, SFTP)
- Incremental and historical ingestion modes
- Built-in error handling and logging
- Credential management via TD secrets

**Slash Commands**:
- `/cdp-ingestion:ingest-new` - Create new source ingestion workflow
- `/cdp-ingestion:ingest-add-klaviyo` - Add Klaviyo source with all objects
- `/cdp-ingestion:ingest-add-object` - Add object to existing source
- `/cdp-ingestion:ingest-validate-wf` - Validate workflow files

**Output**: Digdag workflows (`.dig`), datasource configs (`.yml`), load configs (`.yml`)

---

### 2. CDP Hist-Union (`cdp-histunion`)

**Purpose**: Combine historical and incremental tables into unified tables with watermark-based incremental loading.

**Key Features**:
- Intelligent schema validation via MCP tool
- Automatic detection of schema differences (e.g., `incremental_date` column)
- Support for full-load tables (klaviyo_lists, klaviyo_metric_data)
- Watermark management using inc_log table
- Parallel task execution
- NULL handling for schema mismatches

**Slash Commands**:
- `/cdp-histunion:histunion-create` - Create hist-union for single table
- `/cdp-histunion:histunion-batch` - Batch create for multiple tables
- `/cdp-histunion:histunion-validate` - Validate workflows and SQL

**Output**: SQL files with UNION ALL logic, Digdag workflows with parallel execution

---

### 3. CDP Staging (`cdp-staging`)

**Purpose**: Transform histunion data into staging layer with data quality, standardization, and PII handling.

**Key Features**:
- Schema-driven transformation (uses MCP to get exact schemas)
- Comprehensive data cleansing and standardization
- PII masking and handling
- JSON extraction and flattening
- Deduplication strategies
- Support for both Presto and Hive SQL engines

**Slash Commands**:
- `/cdp-staging:transform-table` - Transform single table to staging format
- `/cdp-staging:transform-batch` - Batch transform multiple tables
- `/cdp-staging:transform-validation` - Validate staging SQL against quality gates

**Output**: Presto/Hive SQL transformation files, Digdag workflows

---

### 4. CDP Unification (`cdp-unification`)

**Purpose**: Implement customer identity resolution and unification to create golden records in Treasure Data.

**Key Features**:
- Live table analysis via MCP (Treasure Data API)
- Automatic extraction of identity keys (email, phone, user_id, etc.)
- Prep table generation for identity matching
- ID graph configuration
- Master record creation

**Slash Commands**:
- `/cdp-unification:unify-setup` - Complete end-to-end ID unification setup
- `/cdp-unification:unify-extract-keys` - Extract identity columns from tables
- `/cdp-unification:unify-create-prep` - Generate prep table files
- `/cdp-unification:unify-create-config` - Generate unification config

**Output**: Prep table SQL files, `unify.yml` config, `id_unification.dig` workflow

---

### 5. CDP Hybrid IDU (`cdp-hybrid-idu`)

**Purpose**: Cross-platform identity unification for Snowflake and Databricks using YAML-driven configuration and intelligent convergence detection.

**Key Features**:
- **Platform Support**: Snowflake and Databricks Delta Lake
- **YAML Configuration**: Single `unify.yml` drives SQL generation for both platforms
- **Intelligent Convergence**: Automatic loop detection stops when ID graph stabilizes
- **Native SQL**: Platform-specific optimizations (Snowflake VARIANT, Databricks Delta)
- **Real-time Execution**: Monitor workflow progress with convergence metrics
- **Key Validation**: Regex patterns and invalid text filtering
- **Master Tables**: Priority-based attribute selection with array support
- **Metadata Tracking**: Complete lineage and column mapping

**Platform-Specific Features**:

**Snowflake**:
- `ARRAY_CONSTRUCT()`, `LATERAL FLATTEN()`, `ARRAY_AGG()`
- `CLUSTER BY` for performance optimization
- `VARIANT` support for flexible data structures
- Native Snowflake connector authentication (password, SSO, key-pair)

**Databricks**:
- Delta Lake table format with ACID transactions
- `COLLECT_LIST()`, `EXPLODE()`, array operations
- Spark SQL optimizations
- Unity Catalog integration

**Slash Commands**:
- `/cdp-hybrid-idu:hybrid-setup` - End-to-end setup with automated YAML creation, SQL generation, and execution
- `/cdp-hybrid-idu:hybrid-unif-config-creator` - Auto-generate unify.yml from live table analysis (Snowflake/Databricks)
- `/cdp-hybrid-idu:hybrid-unif-config-validate` - Validate YAML configuration
- `/cdp-hybrid-idu:hybrid-generate-snowflake` - Generate Snowflake SQL from YAML
- `/cdp-hybrid-idu:hybrid-execute-snowflake` - Execute Snowflake workflow with convergence detection
- `/cdp-hybrid-idu:hybrid-generate-databricks` - Generate Databricks SQL from YAML
- `/cdp-hybrid-idu:hybrid-execute-databricks` - Execute Databricks workflow with convergence detection
- `/cdp-hybrid-idu:hybrid-unif-merge-stats-creator` - Generate professional HTML merge statistics report

**Input**: `unify.yml` with keys, tables, canonical_ids, master_tables

**Output**:
- **SQL Files**: 20+ files (graph creation, loop iterations, canonicalization, enrichment, master tables, metadata)
- **Execution Reports**: Convergence metrics, row counts, timing
- **Tables Created**: ID graphs, lookup tables, enriched tables, master tables

**Convergence Algorithm**:
```
Iteration 1: 14,573 records → 1,565 merged → Continue
Iteration 2: 13,035 records → 15 merged → Continue
Iteration 3: 13,034 records → 1 merged → Continue
Iteration 4: 13,034 records → 0 merged → CONVERGED (Stop)
```

**Example Workflow**:
```bash
# Option 1: Automated end-to-end setup
/cdp-hybrid-idu:hybrid-setup
# Input: Platform (Snowflake/Databricks), tables to analyze, canonical ID name
# Output: Automated YAML creation → SQL generation → Workflow execution
# Result: Complete ID unification with merge statistics

# Option 2: Step-by-step with manual YAML
# 1. Auto-generate YAML configuration
/cdp-hybrid-idu:hybrid-unif-config-creator
# Input: Platform, tables list, canonical ID name
# Output: unify.yml with validated keys and tables

# 2. Generate Snowflake SQL from YAML
/cdp-hybrid-idu:hybrid-generate-snowflake
# Input: unify.yml, database: INDRESH_TEST, schema: PUBLIC
# Output: 22 SQL files in snowflake_sql/unify/

# 3. Execute with convergence detection
/cdp-hybrid-idu:hybrid-execute-snowflake
# Result: 4,940 canonical IDs in 4 iterations (19,512 identities merged)

# 4. Generate merge statistics report
/cdp-hybrid-idu:hybrid-unif-merge-stats-creator
# Input: Platform, database, schema, canonical ID
# Output: Beautiful HTML report with expert analysis (id_unification_report.html)

# 5. Verify results on Snowflake
SELECT * FROM INDRESH_TEST.PUBLIC.td_id_lookup LIMIT 10;
SELECT * FROM INDRESH_TEST.PUBLIC.td_id_master_table LIMIT 10;
```

**New Features (v1.6.0)**:
- **Automated YAML Configuration**: The `hybrid-unif-config-creator` command uses MCP tools to analyze actual Snowflake/Databricks tables, extract user identifiers with strict PII detection, and generate production-ready `unify.yml` configuration automatically
- **Merge Statistics Reporting**: The `hybrid-unif-merge-stats-creator` command generates comprehensive HTML reports with:
  - Executive summary with key metrics (merge ratio, fragmentation reduction)
  - Identity resolution performance analysis
  - Merge distribution patterns and complexity scoring
  - Data quality metrics with coverage percentages
  - Expert recommendations for optimization
  - PDF-ready professional design
- **Improved hybrid-setup**: Now includes automated table analysis and YAML generation as first step

---

## CDP Implementation Flow

### End-to-End Pipeline

```
┌─────────────────────────────────────────────────────────────────────────┐
│ PHASE 1: INGESTION                                                      │
├─────────────────────────────────────────────────────────────────────────┤
│ Source Systems        → Raw Layer (TD)                                  │
│ • Klaviyo            → client_src.klaviyo_events                          │
│ • Shopify            → client_src.shopify_products                        │
│ • BigQuery           → client_src.analytics_users                         │
│ • OneTrust           → client_src.onetrust_profiles                       │
│                                                                          │
│ Tool: /cdp-ingestion:ingest-new                                        │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ PHASE 2: HISTORICAL CONSOLIDATION                                       │
├─────────────────────────────────────────────────────────────────────────┤
│ Hist + Inc Tables    → Unified Tables (TD)                             │
│ klaviyo_events_hist  → client_src.klaviyo_events_histunion               │
│ klaviyo_events       →                                                 │
│                                                                          │
│ Features:                                                               │
│ • Watermark-based incremental loading                                  │
│ • Schema validation and NULL handling                                  │
│ • Parallel processing                                                   │
│                                                                          │
│ Tool: /cdp-histunion:histunion-batch                                   │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ PHASE 3: STAGING TRANSFORMATION                                         │
├─────────────────────────────────────────────────────────────────────────┤
│ Histunion Tables     → Staging Layer (TD)                               │
│ client_src.*_histunion  → client_stg.klaviyo_events             │
│                      → client_stg.shopify_products            │
│                                                                          │
│ Transformations:                                                        │
│ • Data cleansing (trim, case normalization)                            │
│ • PII masking (email, phone)                                           │
│ • JSON extraction                                                       │
│ • Deduplication                                                         │
│ • Data type standardization                                            │
│                                                                          │
│ Tool: /cdp-staging:transform-batch                                     │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ PHASE 4: IDENTITY UNIFICATION                                           │
├─────────────────────────────────────────────────────────────────────────┤
│ Staging Tables       → Golden Records (TD)                              │
│ All *        → client_master.unified_customers                    │
│                                                                          │
│ Process:                                                                │
│ 1. Extract identity keys (email, phone, user_id)                       │
│ 2. Create prep tables for matching                                     │
│ 3. Build identity graph                                                 │
│ 4. Generate master customer records                                     │
│                                                                          │
│ Tool: /cdp-unification:unify-setup                                     │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Usage Guide

### Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd aps_claude_tools
   ```

2. **Install plugins** (in Claude Code):
   ```bash
   /plugin install cdp-ingestion
   /plugin install cdp-staging
   /plugin install cdp-histunion
   /plugin install cdp-unification
   /plugin install cdp-hybrid-idu
   ```

3. **Restart Claude Code** to load plugins

### Typical Implementation Workflow

#### Step 1: Set Up Data Ingestion

```bash
# Create ingestion workflow for Klaviyo
/cdp-ingestion:ingest-add-klaviyo

# Or for custom source
/cdp-ingestion:ingest-new
```

**Input**: Source details, credentials, objects to ingest
**Output**: `ingestion/{source}_ingest_inc.dig`, config files
**Result**: Raw data flowing into `client_src.*` tables

---

#### Step 2: Consolidate Historical Data

```bash
# Create hist-union for multiple tables
/cdp-histunion:histunion-batch

# Provide tables:
# client_src.klaviyo_events, client_src.shopify_products
```

**Input**: List of tables with hist/inc variants
**Output**: `hist_union/queries/{table}.sql`, `hist_union_runner.dig`
**Result**: Unified tables in `client_src.*_histunion`

---

#### Step 3: Transform to Staging

```bash
# Transform multiple tables in batch
/cdp-staging:transform-batch

# Provide list of tables:
# client_src.klaviyo_events_histunion, client_src.shopify_products_histunion
```

**Input**: List of histunion tables
**Output**: `staging/queries/{table}.sql`, workflow files
**Result**: Clean, standardized data in `client_stg.*` tables

---

#### Step 4: Unify Customer Identities

**Option A: Treasure Data (native)**

```bash
# Run complete ID unification setup
/cdp-unification:unify-setup

# Or step by step:
/cdp-unification:unify-extract-keys     # Extract identity columns
/cdp-unification:unify-create-prep      # Create prep tables
/cdp-unification:unify-create-config    # Generate unify.yml
```

**Input**: Database, tables with customer data
**Output**: Prep tables, `unify.yml`, `id_unification.dig`
**Result**: Unified customer records in `client_master.unified_customers`

**Option B: Snowflake (hybrid)**

```bash
# Generate Snowflake SQL from YAML config
/cdp-hybrid-idu:hybrid-generate-snowflake

# Execute with convergence detection
/cdp-hybrid-idu:hybrid-execute-snowflake
```

**Input**: `unify.yml`, Snowflake connection details
**Output**: 20+ SQL files, execution report with convergence metrics
**Result**: Canonical IDs in `td_id_lookup`, enriched tables, master table

**Option C: Databricks (hybrid)**

```bash
# Complete automated setup (recommended)
/cdp-hybrid-idu:hybrid-setup

# Or step-by-step:
# 1. Auto-generate YAML config
/cdp-hybrid-idu:hybrid-unif-config-creator

# 2. Generate Databricks SQL from YAML
/cdp-hybrid-idu:hybrid-generate-databricks

# 3. Execute with convergence detection
/cdp-hybrid-idu:hybrid-execute-databricks

# 4. Generate merge statistics report
/cdp-hybrid-idu:hybrid-unif-merge-stats-creator
```

**Input**: `unify.yml`, Databricks connection details
**Output**: 20+ SQL files, execution report with convergence metrics, HTML statistics report
**Result**: Canonical IDs in Delta Lake tables with master records and comprehensive analytics

---

## Reporting and Analytics

### ID Unification Merge Statistics Reports

The `cdp-hybrid-idu` plugin includes a powerful reporting feature that generates professional HTML reports analyzing ID unification results:

**Command**: `/cdp-hybrid-idu:hybrid-unif-merge-stats-creator`

**Platform Support**: Snowflake and Databricks

**Report Sections**:
1. **Executive Summary** - Key metrics (unified profiles, merge ratio, fragmentation reduction)
2. **Identity Resolution Performance** - Deduplication rates by key type
3. **Merge Distribution Analysis** - Pattern breakdown and complexity scoring
4. **Top Merged Profiles** - Highest complexity identity resolutions
5. **Source Table Configuration** - Column mappings and data sources
6. **Master Table Data Quality** - Coverage percentages for all attributes
7. **Convergence Performance** - Iteration analysis and efficiency metrics
8. **Expert Recommendations** - Strategic guidance and optimization tips
9. **Summary Statistics** - Complete metrics reference

**Features**:
- **Error-Proof Design**: 10 layers of validation ensure zero errors
- **Consistent Output**: Same beautiful report every time
- **Platform-Agnostic**: Works identically for Snowflake and Databricks
- **PDF-Ready**: Print to PDF for stakeholder distribution
- **Expert Analysis**: Data-driven insights and actionable recommendations

**Example**:
```bash
/cdp-hybrid-idu:hybrid-unif-merge-stats-creator

> Platform: Snowflake
> Database: INDRESH_TEST
> Schema: PUBLIC
> Canonical ID: td_id
> Output: (press Enter for default)

✓ Report generated: id_unification_report.html (142 KB)
```

**Sample Metrics from Generated Report**:
- Unified Profiles: 4,940
- Total Identities: 19,512
- Merge Ratio: 3.95:1
- Fragmentation Reduction: 74.7%
- Email Coverage: 100%
- Phone Coverage: 99.39%
- Convergence: 4 iterations
- Data Quality Score: 99.7%

---

### Example: Complete CDP Setup for E-commerce Client

```bash
# 1. Ingest Shopify data
/cdp-ingestion:ingest-new
# Source: Shopify, Objects: products, customers, orders

# 2. Ingest Klaviyo marketing data
/cdp-ingestion:ingest-add-klaviyo
# All Klaviyo objects: events, profiles, campaigns, lists

# 3. Consolidate historical and incremental
/cdp-histunion:histunion-batch
# Tables: shopify_products, shopify_customers, shopify_orders,
#         klaviyo_events, klaviyo_profiles

# 4. Transform histunion tables to staging
/cdp-staging:transform-batch
# Tables: shopify_products_histunion, shopify_customers_histunion,
#         shopify_orders_histunion, klaviyo_events_histunion,
#         klaviyo_profiles_histunion

# 5. Unify customer identities
/cdp-unification:unify-setup
# Database: client_stg
# Tables: shopify_customers, klaviyo_profiles

# Result: Golden customer records ready for analytics and activation
```

---

## Key Principles

### 1. Template-Driven Generation
All plugins use exact, production-tested templates. No improvisation or "improvements" allowed. This ensures:
- Consistency across implementations
- Reduced debugging time
- Proven patterns that work first time

### 2. Schema Validation via MCP
Plugins use MCP (Model Context Protocol) to access Treasure Data API and get exact schemas:
- No manual column listing
- No guessing data types
- Automatic detection of schema differences

### 3. Batch Generation
All files for a task are generated in a SINGLE response:
- User gets complete working solution immediately
- No version mismatches between files
- Ready to deploy and test

### 4. Quality Gates
Every plugin enforces strict quality checks:
- Syntax validation (YAML, SQL)
- Schema compliance
- Template adherence
- Error handling completeness
- Logging presence

### 5. Production-Ready Guarantee
Code generated by these plugins:
- Works the first time
- Follows TD best practices
- Includes comprehensive error handling
- Has complete logging
- Is maintainable and documented

---

## Technology Stack

- **Platforms**:
  - Treasure Data (TD)
  - Snowflake
  - Databricks
- **Workflow Engines**:
  - Digdag (TD)
  - Snowflake Tasks
  - Databricks Jobs
- **Query Engines**:
  - Presto (TD)
  - Hive (TD)
  - Snowflake SQL
  - Spark SQL (Databricks)
- **Storage Formats**:
  - TD Native (Presto/Hive)
  - Snowflake Tables with VARIANT
  - Delta Lake (Databricks)
- **AI Framework**: Claude Code with MCP
- **Version Control**: Git
- **Configuration**: YAML, JSON
- **Authentication**:
  - TD API Keys
  - Snowflake (Password, SSO, Key-Pair)
  - Databricks (Token, OAuth)

---

## Plugin Development

### Creating a New Plugin

1. **Create plugin directory**:
   ```bash
   mkdir -p plugins/my-plugin/{agents,commands,docs}
   ```

2. **Create plugin.json**:
   ```json
   {
     "name": "my-plugin",
     "description": "Plugin description",
     "version": "1.0.0",
     "author": {
       "name": "@cdp-tools-marketplace",
       "organization": "APS CDP Team"
     },
     "prompt": "prompt.md",
     "agents": ["agents/my-expert.md"],
     "commands": ["commands/my-command.md"]
   }
   ```

3. **Create prompt.md** with plugin instructions

4. **Create agent** in `agents/my-expert.md`

5. **Create commands** in `commands/*.md`

6. **Register in marketplace**:
   ```json
   // .claude-plugin/marketplace.json
   {
     "plugins": [
       {
         "name": "my-plugin",
         "source": "./plugins/my-plugin",
         "description": "Brief description"
       }
     ]
   }
   ```

---

## Best Practices

### For Plugin Users

1. **Always validate** before deploying:
   ```bash
   /cdp-{plugin}:validate
   ```

2. **Review generated files** before running workflows

3. **Test in dev environment** first

4. **Use batch commands** for multiple tables to save time

5. **Follow the pipeline order**: ingestion → histunion → staging → unification

### For Plugin Developers

1. **Read existing plugins** to understand patterns

2. **Use exact templates** - never improvise

3. **Enforce quality gates** in validation commands

4. **Document examples** in docs/ directory

5. **Use MCP for live data** access (table schemas, etc.)

6. **Generate all files in ONE response** (batch generation)

---

## Support and Contribution

### Getting Help

- Check plugin-specific `docs/` directories for examples
- Review `prompt.md` for detailed instructions
- Look at existing generated files for patterns

### Contributing

1. Fork the repository
2. Create a feature branch
3. Add/modify plugins following existing patterns
4. Test thoroughly in real TD environment
5. Submit pull request with detailed description

---

## Version History

### v1.6.0 (2025-10-15) - Major Update
**CDP Hybrid IDU Enhancements**:
- ✅ **New Command**: `hybrid-unif-config-creator` - Auto-generate `unify.yml` from live table analysis
  - Uses MCP tools to analyze Snowflake/Databricks tables
  - Strict PII detection (zero tolerance for guessing)
  - Validates data patterns from actual table data
  - Generates production-ready YAML configuration

- ✅ **New Command**: `hybrid-unif-merge-stats-creator` - Professional HTML merge statistics reports
  - 10 layers of error protection (zero chance of error)
  - 9 comprehensive report sections with expert analysis
  - Platform-agnostic (works identically for Snowflake and Databricks)
  - PDF-ready professional design
  - Includes executive summary, performance analysis, data quality metrics

- ✅ **Enhanced**: `hybrid-setup` command now includes automated YAML configuration as first step
  - Complete 3-phase workflow: Config creation → SQL generation → Execution
  - User provides tables, system generates everything automatically

- ✅ **Quality Improvements**: Enhanced SQL generation and documentation consistency

**Quality Improvements**:
- All reports generate identically every time (deterministic)
- Comprehensive error handling with user-friendly messages
- Dynamic column detection for flexible master table structures
- Null-safe calculations (NULLIF protection on all divisions)

### v1.5.0 (2025-10-13)
- Added `cdp-hybrid-idu` plugin for Snowflake and Databricks
- Cross-platform ID unification with convergence detection
- YAML-driven configuration for both platforms

### v1.4.0 (2024-10-13)
- Added `cdp-histunion` plugin
- Historical and incremental data consolidation
- Watermark-based incremental loading

### v1.3.0 (2024-10-13)
- Added `cdp-unification` plugin
- Customer identity resolution for Treasure Data
- ID graph and master record creation

### v1.2.0 (2024-10-13)
- Added `cdp-staging` plugin with Hive support
- Data transformation and quality improvement
- PII handling and JSON extraction

### v1.1.0 (2024-10-10)
- Added `cdp-staging` plugin (Presto only)

### v1.0.0 (2024-10-10)
- Initial release with `cdp-ingestion` plugin
- Support for BigQuery, Klaviyo, Shopify, OneTrust, Pinterest, SFTP

---

## License

Proprietary - APS CDP Team / APS

---

## Contact

For questions, issues, or feature requests:
- **Team**: APS CDP Implementation Team
- **Organization**: Treasure Data (APS)
- **Marketplace**: `@cdp-tools-marketplace`

---

**Built with Claude Code | Powered by AI | Proven in Production**
