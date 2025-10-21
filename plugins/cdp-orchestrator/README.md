# CDP Orchestrator Plugin

## Overview

The **CDP Orchestrator** is an end-to-end automation system that orchestrates your complete Customer Data Platform implementation across all four CDP phases with automated workflow generation, deployment, execution, and monitoring.

### What It Does

```
┌─────────────────────────────────────────────────────────────────┐
│                    CDP COMPLETE PIPELINE                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Phase 1: INGESTION                                             │
│  └─→ Generate → Deploy → Execute → Monitor → Validate ✓        │
│      Creates raw data ingestion workflows from source systems   │
│                                                                 │
│  Phase 2: HIST-UNION                                            │
│  └─→ Generate → Deploy → Execute → Monitor → Validate ✓        │
│      Combines historical + incremental data into unified tables │
│                                                                 │
│  Phase 3: STAGING                                               │
│  └─→ Generate → Deploy → Execute → Monitor → Validate ✓        │
│      Transforms and cleans data with quality improvements       │
│                                                                 │
│  Phase 4: UNIFICATION                                           │
│  └─→ Generate → Deploy → Execute → Monitor → Validate ✓        │
│      Creates unified customer profiles with ID resolution       │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

Total Time: 3-4 hours (depending on data volume)
```

**Each phase MUST complete successfully before proceeding to the next phase** to ensure data dependencies are satisfied.

---

## Key Features

✅ **Fully Automated Execution**
- Generates all workflow files (.dig, .yml, .sql)
- Deploys to Treasure Data
- Executes workflows
- Monitors real-time progress
- Validates data output

✅ **Intelligent Error Handling**
- Auto-fixes common deployment errors
- Retries failed operations (up to 3 attempts)
- Interactive troubleshooting
- Provides clear error messages and solutions

✅ **Data Validation**
- Verifies tables created
- Checks row counts > 0
- Validates schema expectations
- Only proceeds if validation passes

✅ **Real-Time Progress Tracking**
- Shows current phase and step
- Displays elapsed time
- Captures all session IDs
- Updates status every 30 seconds

✅ **Comprehensive Reporting**
- Execution summary
- Data quality metrics
- Session IDs for monitoring
- Next steps guidance

---

## Prerequisites

### 1. System Requirements

#### A. Treasure Data Toolbelt (REQUIRED)

**Check if installed**:
```bash
td --version
```

**Expected output**: `0.16.9` or higher

**If not installed**:

**macOS**:
```bash
brew install td
```

**Linux/Ubuntu**:
```bash
curl -L https://toolbelt.treasuredata.com/sh/install-ubuntu-trusty-td-agent.sh | sh
```

**Windows**:
Download from: https://toolbelt.treasuredata.com/

**Verify installation**:
```bash
td --version
td help
```

---

#### B. Claude Code with Plugin System (REQUIRED)

**Check if plugins are loaded**:
```bash
# From your project directory
ls .claude-plugin/marketplace.json
```

**Expected**: File should exist with CDP plugins registered

**If missing**: Ensure you're running Claude Code with the plugin marketplace enabled.

---

#### C. Treasure Data MCP Server (REQUIRED)

**Purpose**: Enables direct TD query validation without manual commands

**Check if available**:
- The orchestrator can use `mcp__demo_treasuredata__query` tool
- If not available, falls back to `td query` via Bash tool

**Configuration**: See Treasure Data MCP documentation

---

### 2. Treasure Data Prerequisites

#### A. TD API Credentials (REQUIRED)

**What you need**:
- **TD_API_KEY**: Your Treasure Data API key
- **TD_ENDPOINT**: Your regional endpoint URL

**How to get TD_API_KEY**:

1. Go to: https://console.treasuredata.com/app/users
2. Click on your profile
3. Copy **API Key** in format: `{td_account_id}/{api_key_hash}`
4. Example: `12345/abcdef1234567890abcdef1234567890abcdef12`

**Important**:
- ⚠️ Keep this secret - it grants full access to your TD account
- ⚠️ Do NOT commit to git
- ⚠️ Ensure it has write permissions

**How to get TD_ENDPOINT**:

Select based on your region:

| Region | Endpoint URL |
|--------|-------------|
| **US** (default) | `https://api.treasuredata.com` |
| **EU** | `https://api-cdp.eu01.treasuredata.com` |
| **Tokyo** | `https://api-cdp.treasuredata.co.jp` |
| **Asia Pacific** | `https://api-cdp.ap02.treasuredata.com` |

**Verify credentials work**:
```bash
# Replace with your actual credentials
export TD_API_KEY="12345/abcdef..."
export TD_ENDPOINT="https://api.treasuredata.com"

# Test connection
td -k $TD_API_KEY -e $TD_ENDPOINT wf list
```

**Expected**: Should list your workflows (or empty list if none exist)

---

#### B. TD Permissions (REQUIRED)

Your TD account must have permissions to:

- ✅ Create databases
- ✅ Create tables
- ✅ Upload workflows
- ✅ Execute workflows
- ✅ Upload secrets (for data source credentials)
- ✅ Query data

**Check permissions**:
```bash
# Try creating a test database
td -k $TD_API_KEY -e $TD_ENDPOINT db:create test_permissions_db

# If successful, delete it
td -k $TD_API_KEY -e $TD_ENDPOINT db:delete test_permissions_db
```

---

### 3. Data Source Prerequisites

#### A. Source System Access (REQUIRED for Phase 1)

Depending on your data source, you need:

**For Database Sources** (Snowflake, Redshift, BigQuery):
- ✅ Connection credentials (username/password or OAuth)
- ✅ Network access from TD to source
- ✅ Read permissions on source tables
- ✅ Schema/table names identified

**For API Sources** (Salesforce, Shopify, Klaviyo):
- ✅ API keys or OAuth tokens
- ✅ API access enabled
- ✅ Rate limits understood
- ✅ Objects/endpoints identified

**For SFTP/File Sources**:
- ✅ SFTP credentials
- ✅ File paths
- ✅ File formats documented

---

#### B. TD Authentication Created (REQUIRED for Phase 1)

**For most connectors, you need to create a TD Authentication**:

1. Go to TD Console: https://console.treasuredata.com
2. Navigate to: **Integrations → Authentications**
3. Click **+ New Authentication**
4. Select your connector type (e.g., Snowflake, Salesforce)
5. Fill in credentials
6. **Save and note the Authentication ID** (e.g., `360014`)

**Example - Snowflake**:
```yaml
type: Snowflake
account_name: ACCOUNT_NAME-REGION_ID
Username: your_username
Password: your_password
Database: Database
Schema: schema
```

**Save the Authentication ID** - you'll need it for configuration.

---

### 4. Project Setup Prerequisites

#### A. Clean Working Directory (RECOMMENDED)

**Start with a clean project directory**:
```bash
cd /path/to/your/cdp/project

# Ensure no conflicting directories
ls -la
# Should not have: ingestion/, hist_union/, staging/, unification/
# If they exist from previous runs, back them up:
mkdir backup_$(date +%Y%m%d)
mv ingestion hist_union staging unification backup_$(date +%Y%m%d)/ 2>/dev/null
```

---

#### B. Git Repository (OPTIONAL but RECOMMENDED)

**Initialize git for version control**:
```bash
git init
git add .gitignore
git commit -m "Initial commit"
```

**Important**: Ensure `.gitignore` excludes sensitive files:
```gitignore
# Credentials
credentials_*.json
*.env
*_credentials.yml

# Generated workflows (if you want to regenerate)
ingestion/
hist_union/
staging/
unification/

# State files
pipeline_state.json
pipeline_logs/
```

---

### 5. Knowledge Prerequisites

#### A. Understanding Your Data (REQUIRED)

Before starting, you should know:

**Source System**:
- What system are you ingesting from? (e.g., Snowflake, Salesforce)
- What tables/objects contain your data?
- What fields track updates? (for incremental loading)

**Data Relationships**:
- How do tables relate to each other?
- What are the primary/foreign keys?
- What are the user identifiers? (email, customer_id, etc.)

**Business Requirements**:
- What data do you need for customer 360?
- How frequently should data refresh?
- What data quality rules apply?

---

#### B. CDP Concepts (RECOMMENDED)

**Helpful to understand**:

**Ingestion**: Raw data import from source systems
**Hist-Union**: Combining historical + incremental data
**Staging**: Data transformation and quality improvements
**Unification**: ID resolution to create unified customer profiles

**Resources**:
- CDP documentation in `plugins/*/docs/`
- Treasure Data documentation: https://docs.treasuredata.com
- This README's "Phase Details" section below

---

## Input Requirements by Phase

The orchestrator will prompt you for configuration in sequence. Here's what you need to prepare for each phase:

---

### Global Configuration (Required First)

These apply to all phases:

```yaml
# Example values shown
global:
  td_api_key: "12345/abcdef1234567890..."    # REQUIRED
  td_endpoint: "https://api.treasuredata.com" # REQUIRED
  client: "acme"                              # REQUIRED
```

| Parameter | Required | Format | Description | Example |
|-----------|----------|--------|-------------|---------|
| `td_api_key` | **YES** | `{user_id}/{api_key}` | Your TD API key from console | `12345/abcd...` |
| `td_endpoint` | **YES** | URL | Your TD regional endpoint | `https://api.treasuredata.com` |
| `client` | **YES** | Alphanumeric lowercase | Client identifier for database naming | `acme`, `mck`, `clientname` |

**How the orchestrator asks**:
```
Please provide your Treasure Data credentials:

1. TD_API_KEY (required):
   Find at: https://console.treasuredata.com/app/users
   Format: 12345/abcdef1234567890...
   Your API key: _____________

2. TD_ENDPOINT (required):
   - US: https://api.treasuredata.com (default)
   - EU: https://api-cdp.eu01.treasuredata.com
   - Tokyo: https://api-cdp.treasuredata.co.jp
   Your endpoint (or press Enter for US): _____________

3. Client Name (required):
   Your client identifier (e.g., mck, acme)
   Used for database naming
   Your client name: _____________
```

---

### Phase 1: Ingestion Configuration

**Purpose**: Define how to ingest raw data from source systems

```yaml
# Example
ingestion:
  source_name: "snowflake"
  connector: "snowflake"
  objects: ["orders", "customers", "products"]
  mode: "both"
  incremental_field: "updated_at"
  start_date: "2024-01-01T00:00:00.000000"
  target_database: "acme_src"
```

| Parameter | Required | Options/Format | Description | Example |
|-----------|----------|----------------|-------------|---------|
| `source_name` | **YES** | String | System identifier (used in file names) | `snowflake`, `salesforce`, `shopify` |
| `connector` | **YES** | TD connector type | Connector to use for ingestion | `snowflake`, `rest`, `salesforce` |
| `objects` | **YES** | Array of strings | Tables/objects to ingest (comma-separated) | `orders,customers,products` |
| `mode` | **YES** | `incremental` \| `historical` \| `both` | Ingestion mode | `both` (recommended) |
| `incremental_field` | **YES*** | String | Field tracking updates (*required if mode includes incremental) | `updated_at`, `modified_date`, `UPDATE_AT` |
| `start_date` | **YES** | ISO 8601 timestamp | When to start ingesting from | `2024-01-01T00:00:00.000000` |
| `target_database` | NO | String | TD database for raw data (default: `{client}_src`) | `acme_src` |
| `auth_id` | **YES** | Number | TD Authentication ID created in prerequisites | `360014` |

**Additional Connector-Specific Parameters**:

**Snowflake**:
- `account_name` - Snowflake account identifier
- `warehouse` - Warehouse name
- `database` - Snowflake database name
- `schema` - Schema name (default: PUBLIC)

**Salesforce**:
- API version
- Sandbox vs Production

**BigQuery**:
- Project ID
- Dataset

**How the orchestrator asks**:
```
Phase 1: Ingestion Configuration

Data Source:
1. Source name (e.g., shopify, salesforce, snowflake): _____________
2. Connector type (e.g., rest, salesforce, snowflake): _____________
3. Objects/tables (comma-separated, e.g., orders,customers,products): _____________

Settings:
4. Ingestion mode (incremental/historical/both): _____________
5. Incremental field (e.g., updated_at, modified_date): _____________
6. Start date (YYYY-MM-DDTHH:MM:SS.000000): _____________
7. Target database (default: {client}_src, press Enter to use default): _____________
8. TD Authentication ID: _____________
```

**For Snowflake specifically, you'll also be asked**:
```
Snowflake-Specific Configuration:
9. Account name (e.g., ACCOUNT-REGION_ID): _____________
10. Warehouse name: _____________
11. Database name: _____________
12. Schema name (default: PUBLIC): _____________
```

---

### Phase 2: Hist-Union Configuration

**Purpose**: Define which tables need historical + incremental data combined

```yaml
# Example
hist_union:
  tables: ["snowflake_orders", "snowflake_customers", "snowflake_products"]
  source_database: "acme_src"
```

| Parameter | Required | Format | Description | Example |
|-----------|----------|--------|-------------|---------|
| `tables` | **YES** | Array of strings | Tables from Phase 1 to combine (comma-separated) | `shopify_orders,shopify_customers` |
| `source_database` | NO | String | Source database (auto-filled from Phase 1) | `acme_src` |

**Important Notes**:
- Only include tables that have BOTH `_hist` and regular versions
- If mode was `incremental` only in Phase 1, skip this phase
- Table names should match the output from Phase 1

**How the orchestrator asks**:
```
Phase 2: Hist-Union Configuration

Tables to combine (from ingestion output):
- List tables requiring historical + incremental merge
- Format: table_name (comma-separated)
- Example: shopify_orders,shopify_customers

Your tables: _____________
```

---

### Phase 3: Staging Configuration

**Purpose**: Define how to transform and clean data

```yaml
# Example
staging:
  engine: "presto"
  source_database: "acme_src"
  target_database: "acme_stg"
  tables: ["snowflake_orders", "snowflake_customers", "snowflake_products"]
```

| Parameter | Required | Options | Description | Example |
|-----------|----------|---------|-------------|---------|
| `engine` | **YES** | `presto` \| `hive` | SQL engine for transformations | `presto` (recommended) |
| `source_database` | NO | String | Source database (auto-filled from Phase 2) | `acme_src` |
| `target_database` | NO | String | Staging database (default: `{client}_stg`) | `acme_stg` |
| `tables` | NO | Array | Tables to transform (auto-detected from Phase 2) | Auto-filled |

**SQL Engine Selection**:

**Presto** (Recommended):
- ✅ Faster performance
- ✅ More SQL standard compliant
- ✅ Better for modern data types
- ❌ Less compatible with legacy Hive UDFs

**Hive**:
- ✅ Better for legacy compatibility
- ✅ More Hive UDF support
- ❌ Slower performance
- ❌ Less standard SQL

**How the orchestrator asks**:
```
Phase 3: Staging Configuration

Transformation settings:
1. SQL engine (presto/hive, default: presto): _____________
2. Target database (default: {client}_stg): _____________

Tables will be auto-detected from hist-union output.
```

---

### Phase 4: Unification Configuration

**Purpose**: Define how to create unified customer profiles with ID resolution

```yaml
# Example
unification:
  name: "customer_360"
  id_method: "persistent_id"
  update_strategy: "incremental"
  tables: []  # Auto-detected from staging
  regional_endpoint: "https://api.treasuredata.com"
```

| Parameter | Required | Options | Description | Example |
|-----------|----------|---------|-------------|---------|
| `name` | **YES** | String | Unification project name | `customer_360`, `unified_profile` |
| `id_method` | **YES** | `persistent_id` \| `canonical_id` | ID resolution method | `persistent_id` (recommended) |
| `update_strategy` | **YES** | `incremental` \| `full` | Update approach | `full` (recommended) |
| `tables` | NO | Array | Tables from staging (auto-detected) | Auto-filled |
| `regional_endpoint` | NO | URL | Same as global TD_ENDPOINT | Auto-filled |

**ID Method Comparison**:

| Feature | persistent_id | canonical_id |
|---------|--------------|--------------|
| **Stability** | IDs remain stable across updates | IDs may change |
| **Performance** | Faster (only new data) | Slower (full reprocessing) |
| **Use Case** | Production, ongoing updates | One-time analysis |
| **Recommended** | ✅ Yes | For legacy compatibility |

**Update Strategy Comparison**:

| Feature | incremental | full |
|---------|------------|------|
| **Speed** | Fast (only new data) | Slow (all data) |
| **Resource Usage** | Low | High |
| **Use Case** | Daily/hourly updates | Weekly/monthly full refresh |
| **Recommended** | ✅ Yes | For batch processing |

**How the orchestrator asks**:
```
Phase 4: Unification Configuration

Unification settings:
1. Unification name (e.g., customer_360, unified_profile): _____________
2. ID method (persistent_id/canonical_id, default: persistent_id): _____________
3. Update strategy (incremental/full, default: incremental): _____________

Tables will be auto-detected from staging output.
```

---

## Quick Start Guide

### Minimal Configuration Example

Here's the minimum you need to get started:

```yaml
# Save this as config_example.yml (for reference only)
global:
  td_api_key: "YOUR_API_KEY"
  td_endpoint: "https://api.treasuredata.com"
  client: "acme"

ingestion:
  source_name: "shopify"
  connector: "rest"
  objects: ["orders", "customers"]
  mode: "both"
  incremental_field: "updated_at"
  start_date: "2024-01-01T00:00:00.000000"
  auth_id: 360014

hist_union:
  tables: ["shopify_orders", "shopify_customers"]

staging:
  engine: "presto"

unification:
  name: "customer_360"
  id_method: "persistent_id"
  update_strategy: "incremental"
```

---

## Step-by-Step Usage

### Step 1: Prepare Prerequisites

**Checklist**:
- [ ] TD toolbelt installed (`td --version`)
- [ ] TD API key obtained
- [ ] TD Authentication created for data source
- [ ] Source system credentials ready
- [ ] Clean working directory
- [ ] Know your data (tables, fields, relationships)

---

### Step 2: Invoke the Orchestrator

```bash
/cdp-orchestrator:cdp-implement
```

Or if using full plugin name:
```bash
/cdp-orchestrator:cdp-implement
```

---

### Step 3: Provide Configuration

The orchestrator will prompt you step-by-step. You can provide configuration either:

**Option A: Interactively** (Recommended for first-time users)
- Answer each prompt as it appears
- The orchestrator will guide you through each field
- Provides examples and defaults

**Option B: All at Once** (For experienced users)
- Prepare a YAML configuration file
- Provide when prompted: "Provide all at once in YAML format"
- Paste your complete configuration

---

### Step 4: Review Configuration Summary

Before execution, the orchestrator will show:

```markdown
# Configuration Summary

## Global
- TD Endpoint: https://api.treasuredata.com
- Client: acme
- API Key: ****ef12

## Phase 1: Ingestion
- Source: shopify
- Connector: rest
- Objects: orders, customers
- Mode: both
- Database: acme_src

## Phase 2: Hist-Union
- Tables: shopify_orders, shopify_customers
- Database: acme_src

## Phase 3: Staging
- Engine: presto
- Tables: shopify_orders, shopify_customers
- Database: acme_stg

## Phase 4: Unification
- Name: customer_360
- ID Method: persistent_id
- Strategy: incremental

---

Estimated Timeline:
- Phase 1 (Ingestion): ~1 hour
- Phase 2 (Hist-Union): ~30 minutes
- Phase 3 (Staging): ~45 minutes
- Phase 4 (Unification): ~1.5 hours
- Total: 3-4 hours

Ready to proceed? (yes/no):
```

Type `yes` to start execution.

---

### Step 5: Monitor Execution

The orchestrator will automatically:

1. **Generate** workflows for each phase
2. **Deploy** to Treasure Data
3. **Execute** workflows
4. **Monitor** progress in real-time
5. **Validate** output data
6. **Proceed** to next phase

**Real-time progress display**:
```
✓ Pre-Flight: Configuration gathered
→ Phase 1: Ingestion
  ✓ Generate workflows
  ✓ Deploy workflows
  ✓ Execute workflows
  ⏳ Monitor execution... (15:30 elapsed)
     Status: running
     Session ID: 123456789
     Checking again in 30 seconds...
  □ Validate output
□ Phase 2: Hist-Union
□ Phase 3: Staging
□ Phase 4: Unification
□ Final: Report generation
```

**Status Indicators**:
- ✓ Completed
- → In Progress
- ⏳ Waiting/Monitoring
- □ Pending
- ✗ Failed

---

### Step 6: Handle Errors (if any)

If a phase fails, the orchestrator will:

1. **Show the error**:
```
✗ Phase 1 Failed

Workflow: shopify_ingest_inc
Session ID: 123456789
Error: Table 'shopify_orders' not found

Possible Causes:
1. Source table doesn't exist
2. Incorrect table name
3. Missing permissions

Options:
1. Retry - Run workflow again
2. Fix - Help me fix the issue
3. View Full Logs - See complete execution logs
4. Skip - Skip this phase (NOT RECOMMENDED)
5. Abort - Stop entire pipeline

Your choice (1-5): _
```

2. **Wait for your decision**
3. **Take action** based on your choice
4. **Retry or proceed** as directed

**Common auto-fixes**:
- Syntax errors in YAML → Auto-fixed
- Missing databases → Created if you approve
- Transient network errors → Retried automatically

---

### Step 7: Review Final Report

Upon completion, you'll receive a comprehensive report:

```markdown
# CDP Implementation Complete ✓

Pipeline ID: 20251021-143215
Started: 2025-10-21 14:32:15
Completed: 2025-10-21 18:15:30
Total Duration: 3h 43m 15s

---

## Pipeline Execution Summary

| Phase | Status | Duration | Tables | Rows | Session ID |
|-------|--------|----------|--------|------|------------|
| Ingestion | ✓ Success | 1h 05m | 2 | 1.2M | 123456789 |
| Hist-Union | ✓ Success | 28m | 2 | 1.2M | 123456790 |
| Staging | ✓ Success | 42m | 2 | 1.2M | 123456791 |
| Unification | ✓ Success | 1h 28m | 6 | 1.2M | 123456792 |
| TOTAL | ✓ | 3h 43m | 12 | 4.8M | - |

---

## Files Generated

Total Files: 47

### Phase 1: Ingestion (11 files)
...

### Phase 2: Hist-Union (8 files)
...

[Complete file listing]

---

## Data Quality Metrics

### ID Unification Results
- Unique customer IDs: 485,293
- ID resolution rate: 94.3%
- Average IDs per customer: 2.4
- Coverage: 98.1%

---

## Next Steps

1. Set up scheduling
2. Create monitoring dashboard
3. Review data quality
4. Document for operations

[Detailed next steps]
```

---

## Phase Details

### Phase 1: Ingestion - Deep Dive

**What happens**:

1. **Generation** (2-5 minutes):
   - Invokes `/cdp-ingestion:ingest-new`
   - Creates workflow files:
     - `{source}_ingest_inc.dig` - Incremental workflow
     - `{source}_ingest_hist.dig` - Historical workflow (if mode=both)
     - `config/{source}_datasources.yml` - Datasource configuration
     - `config/{source}_{object}_load.yml` - Per-object load configs
     - `config/hist_date_ranges.yml` - Monthly date ranges (if mode=both/historical)
   - Files created in `ingestion/` directory

2. **Deployment** (30 seconds):
   - Navigates to `ingestion/` directory
   - Executes: `td -k $TD_API_KEY -e $TD_ENDPOINT wf push ingestion`
   - Validates syntax
   - Auto-fixes common errors
   - Retries up to 3 times if needed

3. **Execution** (15-60 minutes):
   - Starts workflow: `td  -k $TD_API_KEY -e $TD_ENDPOINT wf start ingestion {source}_ingest_inc`
   - If mode=both, runs historical first, then incremental
   - Captures session IDs

4. **Monitoring** (until completion):
   - Polls status every 30 seconds
   - Shows real-time progress
   - Displays elapsed time
   - Continues until status is "success" or "error"

5. **Validation** (1 minute):
   - Queries TD: `SELECT * FROM information_schema.tables WHERE database_name = '{target_db}'`
   - Verifies tables created for each object
   - Checks row counts > 0
   - Queries ingestion log for status

**Success Criteria**:
- ✅ All object tables created
- ✅ Row counts > 0
- ✅ Ingestion log shows SUCCESS

**Proceeds to Phase 2** only if validation passes.

---

### Phase 2: Hist-Union - Deep Dive

**What happens**:

1. **Generation** (1-2 minutes):
   - Invokes `/cdp-histunion:histunion-batch`
   - Creates workflow files:
     - `{source}_hist_union.dig` - Main workflow
     - `queries/{table}_union.sql` - Union SQL per table
   - Files created in `hist_union/` directory

2. **Deployment** (30 seconds):
   - Navigates to `hist_union/` directory
   - Executes: `td -k $TD_API_KEY -e $TD_ENDPOINT wf push hist_union`
   - Validates syntax

3. **Execution** (10-30 minutes):
   - Starts workflow: `td -k $TD_API_KEY -e $TD_ENDPOINT wf start hist_union {source}_hist_union`
   - Combines `{table}_hist` + `{table}` → `{table}_hist_union`

4. **Monitoring** (until completion):
   - Polls status every 30 seconds
   - Shows progress

5. **Validation** (1 minute):
   - Queries for `*_hist_union` or `*_union` tables
   - Verifies row counts >= source table counts
   - Checks for duplicates

**Success Criteria**:
- ✅ All union tables created
- ✅ Row counts >= source tables
- ✅ No unexpected duplicates

**Proceeds to Phase 3** only if validation passes.

---

### Phase 3: Staging - Deep Dive

**What happens**:

1. **Generation** (2-5 minutes):
   - Invokes `/cdp-staging:transform-batch`
   - Creates workflow files:
     - `transform_{source}.dig` - Main workflow
     - `queries/{table}_transform.sql` - Transform SQL per table
   - Files created in `staging/` directory
   - Applies:
     - Data type standardization
     - PII handling (hashing, masking)
     - JSON extraction
     - Null handling
     - Data quality improvements

2. **Deployment** (30 seconds):
   - Navigates to `staging/` directory
   - Executes: `td -k $TD_API_KEY -e $TD_ENDPOINT wf push staging`

3. **Execution** (20-45 minutes):
   - Starts workflow: `td -k $TD_API_KEY -e $TD_ENDPOINT wf start staging transform_{source}`
   - Processes each table through transformation SQL

4. **Monitoring** (until completion):
   - Polls status every 30 seconds

5. **Validation** (1 minute):
   - Queries for staging tables (`*_stg` or `*_stg_*`)
   - Verifies transformed columns exist
   - Checks schema matches expectations

**Success Criteria**:
- ✅ All staging tables created
- ✅ Transformation columns added
- ✅ Data quality improvements applied

**Proceeds to Phase 4** only if validation passes.

---

### Phase 4: Unification - Deep Dive

**What happens**:

1. **Generation** (3-5 minutes):
   - Invokes `/cdp-unification:unify-setup`
   - Creates workflow files:
     - `unif_runner.dig` - Main orchestrator
     - `dynmic_prep_creation.dig` - Prep table creation
     - `id_unification.dig` - ID resolution via API
     - `enrich_runner.dig` - Enrichment
     - `config/unify.yml` - Configuration
     - `config/environment.yml` - Client environment
     - `config/src_prep_params.yml` - Prep parameters
     - `config/stage_enrich.yml` - Enrichment config
     - 15+ SQL query files
   - Files created in `unification/` directory

2. **Deployment** (30 seconds):
   - Navigates to `unification/` directory
   - Executes: `td -k $TD_API_KEY -e $TD_ENDPOINT wf push unification`

3. **Execution** (30-90 minutes):
   - Starts workflow: `td -k $TD_API_KEY -e $TD_ENDPOINT wf start unification unif_runner`
   - Sub-workflows:
     - Creates prep tables
     - Calls TD ID Unification API
     - Enriches source tables with unified_id
   - Longest phase due to API calls

4. **Monitoring** (until completion):
   - Polls status every 30 seconds
   - May show multiple sub-workflow executions

5. **Validation** (2 minutes):
   - Queries for unification tables:
     - `{client}_{name}_prep`
     - `{client}_{name}_unified_id_lookup`
     - `{client}_{name}_unified_id_graph`
     - `{table}_enriched` (for each table)
   - Verifies unified_id coverage
   - Checks resolution statistics

**Success Criteria**:
- ✅ All unification tables created
- ✅ unified_id_lookup has data
- ✅ Enriched tables have unified_id column
- ✅ Coverage > 90%

**Completes pipeline** if validation passes.

---

## Monitoring & Troubleshooting

### Real-Time Monitoring

**During execution, you can monitor**:

1. **In Claude Code**:
   - Real-time progress updates
   - Status changes
   - Elapsed time
   - Session IDs

2. **In TD Console**:
   ```
   https://console.treasuredata.com/app/workflows
   ```
   - View running workflows
   - See task progress
   - Check logs

3. **Via TD CLI**:
   ```bash
   # List recent sessions
   td -k $TD_API_KEY -e $TD_ENDPOINT wf sessions

   # Check specific session
   td -k $TD_API_KEY -e $TD_ENDPOINT wf session {session_id}

   # View logs
   td -k $TD_API_KEY -e $TD_ENDPOINT wf log {session_id}
   ```

---

### Common Issues & Solutions

#### Issue 1: Authentication Failed

**Symptom**:
```
Error: authentication failed: Invalid API key
```

**Cause**: Invalid or expired TD_API_KEY

**Solution**:
1. Verify API key format: `{user_id}/{api_key}`
2. Check for extra spaces or newlines
3. Get fresh API key from TD console
4. Test manually:
   ```bash
   td -k YOUR_API_KEY -e YOUR_ENDPOINT wf list
   ```

---

#### Issue 2: Database Not Found

**Symptom**:
```
Error: database 'acme_src' does not exist
```

**Cause**: Target database doesn't exist

**Solution**:
- The orchestrator will ask if you want to create it
- Choose "yes" to auto-create
- Or create manually:
  ```bash
  td -k $TD_API_KEY -e $TD_ENDPOINT db:create acme_src
  ```

---

#### Issue 3: Table Not Found During Execution

**Symptom**:
```
Error: Table 'acme_src.shopify_orders' does not exist
```

**Cause**: Previous phase failed to create expected table

**Solution**:
1. Check previous phase logs
2. Verify ingestion completed successfully
3. Query TD to confirm:
   ```sql
   SELECT * FROM information_schema.tables
   WHERE database_name = 'acme_src'
   ```
4. Re-run previous phase if needed

---

#### Issue 4: Workflow Syntax Error

**Symptom**:
```
Error: syntax error in shopify_ingest_inc.dig:15: unexpected token
```

**Cause**: Generated workflow has syntax issues

**Solution**:
- The orchestrator will attempt auto-fix
- If auto-fix fails, check the file manually
- Common fixes:
  - Missing colons after `td>`
  - Incorrect indentation
  - Quote issues in SQL strings

---

#### Issue 5: Timeout During Monitoring

**Symptom**:
```
Workflow has been running for over 2 hours
```

**Cause**: Large data volume or slow source system

**Solution**:
1. Choose "Continue Waiting" to keep monitoring
2. Check TD console for actual workflow status
3. Review workflow logs for bottlenecks
4. Consider optimizing query or reducing data volume

---

#### Issue 6: Low ID Resolution Rate

**Symptom**:
```
ID resolution rate: 45.2% (expected > 90%)
```

**Cause**: Poor data quality or missing identifiers

**Solution**:
1. Review source data quality
2. Check for NULL values in key fields
3. Verify identifier columns are correct
4. Add more identifier mappings in unify.yml
5. Query to investigate:
   ```sql
   SELECT
     COUNT(*) as total,
     COUNT(email) as has_email,
     COUNT(customer_id) as has_id
   FROM acme_stg.shopify_customers
   ```

---

## FAQ

### General Questions

**Q: How long does the full pipeline take?**

A: Typically 3-4 hours for initial run, depending on:
- Data volume (more data = longer)
- Number of tables (more tables = longer)
- Source system speed (slower API = longer)
- TD warehouse size (larger = faster)

Breakdown:
- Ingestion: 1 hour
- Hist-Union: 30 minutes
- Staging: 45 minutes
- Unification: 1.5 hours

---

**Q: Can I stop and resume the pipeline?**

A: Partially. You can:
- ✅ Stop at any phase boundary
- ✅ Re-run from any phase
- ✅ Skip completed phases
- ❌ Cannot pause mid-workflow execution

To resume:
1. The orchestrator maintains state in `pipeline_state.json`
2. On restart, it detects completed phases
3. Prompts to skip or re-run completed phases

---

**Q: What if I need to change configuration mid-execution?**

A:
- Stop the pipeline (Abort option)
- Generated files remain in place
- Edit configuration files directly
- Re-run orchestrator
- Choose to skip completed phases

---

**Q: Can I run phases manually instead of using orchestrator?**

A: Yes! You can use individual commands:

```bash
# Phase 1
/cdp-ingestion:ingest-new

# Phase 2
/cdp-histunion:histunion-batch

# Phase 3
/cdp-staging:transform-batch

# Phase 4
/cdp-unification:unify-setup
```

Then deploy and execute manually:
```bash
cd ingestion
td -k $TD_API_KEY -e $TD_ENDPOINT wf push ingestion
td -k $TD_API_KEY -e $TD_ENDPOINT wf start ingestion shopify_ingest_inc
```

---

### Technical Questions

**Q: What files does the orchestrator generate?**

A: Complete file listing by phase:

**Ingestion** (~10-15 files):
- Workflow files: `{source}_ingest_inc.dig`, `{source}_ingest_hist.dig`
- Config files: `config/database.yml`, `config/{source}_datasources.yml`, `config/hist_date_ranges.yml`
- Load configs: `config/{source}_{object}_load.yml` (per object)
- SQL files: `sql/log_ingestion_*.sql`

**Hist-Union** (~5-10 files):
- Workflow: `{source}_hist_union.dig`
- SQL: `queries/{table}_union.sql` (per table)

**Staging** (~5-10 files):
- Workflow: `transform_{source}.dig`
- SQL: `queries/{table}_transform.sql` (per table)

**Unification** (~25-30 files):
- Workflows: `unif_runner.dig`, `dynmic_prep_creation.dig`, `id_unification.dig`, `enrich_runner.dig`
- Configs: `config/*.yml` (4 files)
- SQL: `queries/*.sql` (15+ files)

**Total**: 50-70 files

---

**Q: What databases does the orchestrator create?**

A: It uses/creates these databases:

- `{client}_src` - Source/raw data (Phase 1)
- `{client}_stg` - Staging/transformed data (Phase 3)
- `{client}_gld` - Gold/master data (Phase 4 - optional)
- `cdp_unification_{client}` - Unification metadata (Phase 4)

You can customize database names when prompted.

---

**Q: How do I schedule the workflows after completion?**

A: The final report includes scheduling commands:

```bash
# Schedule incremental ingestion (daily at 2 AM)
td -k $TD_API_KEY -e $TD_ENDPOINT wf schedule ingestion shopify_ingest_inc "0 2 * * *"

# Schedule unification refresh (daily at 4 AM)
td -k $TD_API_KEY -e $TD_ENDPOINT wf schedule unification unif_runner "0 4 * * *"

# View all schedules
td -k $TD_API_KEY -e $TD_ENDPOINT wf schedules
```

---

**Q: What monitoring queries should I set up?**

A: Essential monitoring queries:

**Last Ingestion Time**:
```sql
SELECT MAX(time) as last_ingestion_timestamp,
       TD_TIME_FORMAT(MAX(time), 'yyyy-MM-dd HH:mm:ss', 'UTC') as last_ingestion
FROM acme_src.ingestion_log
WHERE source_name = 'shopify'
  AND status = 'SUCCESS'
```

**Recent Workflow Failures**:
```sql
SELECT source_name, table_name, error_message, end_time
FROM acme_src.ingestion_log
WHERE status = 'ERROR'
ORDER BY time DESC
LIMIT 10
```

**ID Resolution Stats**:
```sql
SELECT *
FROM acme_stg.unified_id_result_key_stats
WHERE from_table = '*'
ORDER BY time DESC
LIMIT 1
```

**Data Freshness**:
```sql
SELECT
  table_name,
  MAX(updated_at) as latest_record,
  COUNT(*) as total_records
FROM acme_src.shopify_orders
GROUP BY table_name
```

---

**Q: How do I troubleshoot failed workflows?**

A: Step-by-step debugging:

1. **Get Session ID** (from orchestrator output)

2. **Check Status**:
   ```bash
   td -k $TD_API_KEY -e $TD_ENDPOINT wf session {session_id}
   ```

3. **View Logs**:
   ```bash
   td -k $TD_API_KEY -e $TD_ENDPOINT wf log {session_id} > workflow_log.txt
   ```

4. **Search for Error**:
   ```bash
   grep -i error workflow_log.txt
   grep -i exception workflow_log.txt
   grep -i failed workflow_log.txt
   ```

5. **Check Specific Task**:
   ```bash
   td -k $TD_API_KEY -e $TD_ENDPOINT wf log {session_id} --task +load_data
   ```

6. **Query Ingestion Log**:
   ```sql
   SELECT * FROM acme_src.ingestion_log
   WHERE workflow_name = 'shopify_ingest_inc'
   ORDER BY time DESC
   LIMIT 10
   ```

---

## Best Practices

### Before Starting

✅ **Test credentials first**:
```bash
td -k $TD_API_KEY -e $TD_ENDPOINT wf list
```

✅ **Start with small dataset**:
- Use recent date for `start_date`
- Test with 1-2 objects first
- Expand after successful test

✅ **Backup existing workflows**:
```bash
mkdir backup_$(date +%Y%m%d)
mv ingestion hist_union staging unification backup_$(date +%Y%m%d)/
```

✅ **Document your configuration**:
- Save your input values
- Note the session IDs
- Record any custom changes

---

### During Execution

✅ **Monitor actively**:
- Watch for errors
- Check TD console periodically
- Note session IDs

✅ **Don't interrupt workflows**:
- Let phases complete
- Don't kill TD processes
- Use Abort option if needed

✅ **Save logs**:
- Copy session IDs
- Save error messages
- Screenshot important info

---

### After Completion

✅ **Validate data quality**:
```sql
-- Check record counts
SELECT COUNT(*) FROM acme_src.shopify_orders;

-- Check ID coverage
SELECT
  COUNT(*) as total,
  COUNT(unified_id) as with_id,
  COUNT(unified_id) * 100.0 / COUNT(*) as coverage_pct
FROM acme_stg.shopify_orders_enriched;
```

✅ **Set up schedules**:
- Schedule incremental ingestion
- Schedule unification refresh
- Set up monitoring alerts

✅ **Document for operations**:
- Create runbook for daily operations
- Document troubleshooting steps
- Share session IDs with team

---

## Examples

### Example 1: Snowflake Ingestion

**Scenario**: Ingest orders and customers from Snowflake

**Configuration**:
```yaml
global:
  td_api_key: "12345/abcd..."
  td_endpoint: "https://api.treasuredata.com"
  client: "acme"

ingestion:
  source_name: "snowflake"
  connector: "snowflake"
  objects: ["ORDERS", "CUSTOMERS"]
  mode: "both"
  incremental_field: "UPDATED_AT"
  start_date: "2024-01-01 00:00:00.000"
  target_database: "acme_src"
  auth_id: 360014
  account_name: "ACCOUNT-REGION"
  warehouse: "COMPUTE_WH"
  database: "PROD_DB"
  schema: "PUBLIC"

hist_union:
  tables: ["snowflake_ORDERS", "snowflake_CUSTOMERS"]

staging:
  engine: "presto"

unification:
  name: "customer_360"
  id_method: "persistent_id"
  update_strategy: "incremental"
```

**Expected Output**:
- 2 ingestion tables (orders, customers)
- 2 hist-union tables
- 2 staging tables
- 6 unification tables (prep, lookup, graph, 2 enriched, master)

---

### Example 2: Salesforce API Ingestion

**Scenario**: Ingest Accounts, Contacts, Opportunities from Salesforce

**Configuration**:
```yaml
global:
  td_api_key: "12345/abcd..."
  td_endpoint: "https://api.treasuredata.com"
  client: "salesforce_client"

ingestion:
  source_name: "salesforce"
  connector: "salesforce"
  objects: ["Account", "Contact", "Opportunity"]
  mode: "both"
  incremental_field: "LastModifiedDate"
  start_date: "2024-01-01T00:00:00.000000"
  target_database: "salesforce_client_src"
  auth_id: 360020

hist_union:
  tables: ["salesforce_Account", "salesforce_Contact", "salesforce_Opportunity"]

staging:
  engine: "presto"

unification:
  name: "crm_unified"
  id_method: "persistent_id"
  update_strategy: "incremental"
```

---

## Support & Resources

### Documentation

- **Plugin Documentation**: `plugins/*/docs/`
- **Workflow Patterns**: `plugins/cdp-ingestion/docs/patterns/workflow-patterns.md`
- **Logging Patterns**: `plugins/cdp-ingestion/docs/patterns/logging-patterns.md`
- **Timestamp Formats**: `plugins/cdp-ingestion/docs/patterns/timestamp-formats.md`

### Treasure Data Resources

- **Console**: https://console.treasuredata.com
- **Documentation**: https://docs.treasuredata.com
- **Connector Docs**: https://docs.treasuredata.com/display/public/INT/

### Getting Help

1. **Check logs**: Review workflow logs for error details
2. **Query ingestion_log**: Check status and errors
3. **Review this README**: Check FAQ and troubleshooting
4. **Contact support**: Provide session IDs and error messages

---

## Appendix

### A. Complete Command Reference

```bash
# Invoke orchestrator
/cdp-orchestrator:cdp-implement

# Individual phase commands
/cdp-ingestion:ingest-new
/cdp-histunion:histunion-batch
/cdp-staging:transform-batch
/cdp-unification:unify-setup

# TD toolbelt commands
td --version
td -k API_KEY -e ENDPOINT wf list
td -k API_KEY -e ENDPOINT wf push PROJECT
td -k API_KEY -e ENDPOINT wf start PROJECT WORKFLOW
td -k API_KEY -e ENDPOINT wf session SESSION_ID
td -k API_KEY -e ENDPOINT wf log SESSION_ID
td -k API_KEY -e ENDPOINT wf schedules
td -k API_KEY -e ENDPOINT db:create DATABASE
td -k API_KEY -e ENDPOINT db:list
```

---

### B. File Structure After Completion

```
project/
├── ingestion/
│   ├── snowflake_ingest_inc.dig
│   ├── snowflake_ingest_hist.dig
│   ├── config/
│   │   ├── database.yml
│   │   ├── hist_date_ranges.yml
│   │   ├── snowflake_datasources.yml
│   │   └── snowflake_ORDERS_load.yml
│   └── sql/
│       ├── log_ingestion_start.sql
│       ├── log_ingestion_success.sql
│       └── log_ingestion_error.sql
├── hist_union/
│   ├── snowflake_hist_union.dig
│   └── queries/
│       ├── snowflake_ORDERS_union.sql
│       └── snowflake_CUSTOMERS_union.sql
├── staging/
│   ├── transform_snowflake.dig
│   └── queries/
│       ├── snowflake_ORDERS_transform.sql
│       └── snowflake_CUSTOMERS_transform.sql
├── unification/
│   ├── unif_runner.dig
│   ├── dynmic_prep_creation.dig
│   ├── id_unification.dig
│   ├── enrich_runner.dig
│   ├── config/
│   │   ├── unify.yml
│   │   ├── environment.yml
│   │   ├── src_prep_params.yml
│   │   └── stage_enrich.yml
│   └── queries/
│       └── (15+ SQL files)
├── pipeline_state.json
├── pipeline_report.md
└── pipeline_logs/
    └── 20251021/
        ├── phase1_ingestion.log
        ├── phase2_histunion.log
        ├── phase3_staging.log
        └── phase4_unification.log
```

---

### C. Database Schema After Completion

**Database: {client}_src**
```
Tables:
- {source}_{object}_hist (if mode=both)
- {source}_{object}
- {source}_{object}_hist_union
- ingestion_log
```

**Database: {client}_stg**
```
Tables:
- {source}_stg_{object}
```

**Database: cdp_unification_{client}**
```
Tables:
- {client}_{name}_prep
- {client}_{name}_unified_id_lookup
- {client}_{name}_unified_id_graph
- {client}_{name}_master
- {object}_enriched (for each staging table)
- unified_id_result_key_stats
```

---

## Version Information

**CDP Orchestrator Plugin Version**: 1.0.0
**Last Updated**: 2025-10-21
**Compatible TD Toolbelt**: 0.16.9+
**Compatible Claude Code**: Latest

---

## License & Attribution

Part of the CDP Tools Marketplace
Maintained by: CDP Tools Team

---

**End of README**

For issues or questions about this plugin, refer to:
- Plugin documentation in `plugins/cdp-orchestrator/`
- Individual plugin docs in `plugins/cdp-*/docs/`
- Treasure Data support: https://support.treasuredata.com
