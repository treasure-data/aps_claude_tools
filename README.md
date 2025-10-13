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
    │ • ID Resolution  │  │ • YAML Config    │  │ • YAML Config    │
    │ • Entity Merge   │  │ • IDU SQL Gen    │  │ • IDU SQL Gen    │
    │ • Master Records │  │ • IDU Execution  │  │ • IDU Execution  │
    │ • TD Workflow    │  │                  │  │                  │
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
│       │   ├── snowflake-sql-generator.md
│       │   ├── snowflake-workflow-executor.md
│       │   ├── databricks-sql-generator.md
│       │   └── databricks-workflow-executor.md
│       ├── commands/
│       │   ├── hybrid-setup.md
│       │   ├── hybrid-validate.md
│       │   ├── hybrid-generate-snowflake.md
│       │   ├── hybrid-execute-snowflake.md
│       │   ├── hybrid-generate-databricks.md
│       │   └── hybrid-execute-databricks.md
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
- `/cdp-hybrid-idu:hybrid-setup` - End-to-end setup with YAML creation
- `/cdp-hybrid-idu:hybrid-validate` - Validate YAML configuration
- `/cdp-hybrid-idu:hybrid-generate-snowflake` - Generate Snowflake SQL from YAML
- `/cdp-hybrid-idu:hybrid-execute-snowflake` - Execute Snowflake workflow
- `/cdp-hybrid-idu:hybrid-generate-databricks` - Generate Databricks SQL from YAML
- `/cdp-hybrid-idu:hybrid-execute-databricks` - Execute Databricks workflow

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
# 1. Generate Snowflake SQL from YAML
/cdp-hybrid-idu:hybrid-generate-snowflake
# Input: unify.yml, database: INDRESH_TEST, schema: PUBLIC

# 2. Execute with convergence detection
/cdp-hybrid-idu:hybrid-execute-snowflake
# Result: 13,033 canonical IDs in 4 iterations (60% faster than max 10)

# 3. Verify results on Snowflake.
SELECT * FROM INDRESH_TEST.PUBLIC.td_id_lookup LIMIT 10;
```

---

## CDP Implementation Flow

### End-to-End Pipeline

```
┌─────────────────────────────────────────────────────────────────────────┐
│ PHASE 1: INGESTION                                                      │
├─────────────────────────────────────────────────────────────────────────┤
│ Source Systems        → Raw Layer (TD)                                  │
│ • Klaviyo            → mck_src.klaviyo_events                          │
│ • Shopify            → mck_src.shopify_products                        │
│ • BigQuery           → mck_src.analytics_users                         │
│ • OneTrust           → mck_src.onetrust_profiles                       │
│                                                                          │
│ Tool: /cdp-ingestion:ingest-new                                        │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ PHASE 2: HISTORICAL CONSOLIDATION                                       │
├─────────────────────────────────────────────────────────────────────────┤
│ Hist + Inc Tables    → Unified Tables (TD)                             │
│ klaviyo_events_hist  → mck_src.klaviyo_events_histunion               │
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
│ mck_src.*_histunion  → mck_staging.klaviyo_events_staging             │
│                      → mck_staging.shopify_products_staging            │
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
│ All *_staging        → mck_master.unified_customers                    │
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
**Result**: Raw data flowing into `mck_src.*` tables

---

#### Step 2: Consolidate Historical Data

```bash
# Create hist-union for multiple tables
/cdp-histunion:histunion-batch

# Provide tables:
# mck_src.klaviyo_events, mck_src.shopify_products
```

**Input**: List of tables with hist/inc variants
**Output**: `hist_union/queries/{table}.sql`, `hist_union_runner.dig`
**Result**: Unified tables in `mck_src.*_histunion`

---

#### Step 3: Transform to Staging

```bash
# Transform multiple tables in batch
/cdp-staging:transform-batch

# Provide list of tables:
# mck_src.klaviyo_events_histunion, mck_src.shopify_products_histunion
```

**Input**: List of histunion tables
**Output**: `staging/queries/{table}_staging.sql`, workflow files
**Result**: Clean, standardized data in `mck_staging.*` tables

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
**Result**: Unified customer records in `mck_master.unified_customers`

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
# Generate Databricks SQL from YAML config
/cdp-hybrid-idu:hybrid-generate-databricks

# Execute with convergence detection
/cdp-hybrid-idu:hybrid-execute-databricks
```

**Input**: `unify.yml`, Databricks connection details
**Output**: 20+ SQL files, execution report with convergence metrics
**Result**: Canonical IDs in Delta Lake tables with master records

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
# Database: mck_staging
# Tables: shopify_customers_staging, klaviyo_profiles_staging

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

- **v1.5.0** (2025-10-13): Added `cdp-hybrid-idu` plugin for Snowflake and Databricks
- **v1.4.0** (2024-10-13): Added `cdp-histunion` plugin
- **v1.3.0** (2024-10-13): Added `cdp-unification` plugin
- **v1.2.0** (2024-10-13): Added `cdp-staging` plugin with Hive support
- **v1.1.0** (2024-10-10): Added `cdp-staging` plugin
- **v1.0.0** (2024-10-10): Initial release with `cdp-ingestion` plugin

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
