---
name: cdp-implement
description: Complete CDP implementation with workflow generation, deployment, and execution
---

# CDP Complete Implementation Pipeline

## Overview

I'll orchestrate your complete CDP implementation with **automated deployment and execution** across all phases:

```
Phase 1: Ingestion      → Generate → Deploy → Execute → Monitor → Validate ✓
Phase 2: Hist-Union     → Generate → Deploy → Execute → Monitor → Validate ✓
Phase 3: Staging        → Generate → Deploy → Execute → Monitor → Validate ✓
Phase 4: Unification    → Generate → Deploy → Execute → Monitor → Validate ✓
```

**Each phase MUST complete successfully before proceeding to the next phase.**

This ensures data dependencies are satisfied:
- Ingestion creates source tables
- Hist-Union requires source tables
- Staging requires hist-union tables
- Unification requires staging tables

---

## Required Inputs

### Global Configuration (Required First)

**TD API Credentials:**
- **TD_API_KEY**: Your Treasure Data API key (required for deployment & execution)
  - Find at: https://console.treasuredata.com/app/users
  - Format: `12345/abcdef1234567890abcdef1234567890abcdef12`

- **TD_ENDPOINT**: Your TD regional endpoint
  - **US**: `https://api.treasuredata.com` (default)
  - **EU**: `https://api-cdp.eu01.treasuredata.com`
  - **Tokyo**: `https://api-cdp.treasuredata.co.jp`
  - **Asia Pacific**: `https://api-cdp.ap02.treasuredata.com`

**Client Information:**
- **Client Name**: Your client identifier (e.g., `mck`, `acme`, `client_name`)
  - Used for: Database naming, configuration, organization

---

### Phase 1: Ingestion Configuration

**Data Source:**
- **Source Name**: System name (e.g., `shopify`, `salesforce`, `klaviyo`)
- **Connector Type**: TD connector (e.g., `rest`, `salesforce`, `bigquery`)
- **Objects/Tables**: Comma-separated list (e.g., `orders,customers,products`)

**Ingestion Settings:**
- **Mode**: Choose one:
  - `incremental` - Ongoing data sync only
  - `historical` - One-time historical backfill only
  - `both` - Separate historical and incremental workflows (recommended)

- **Incremental Field**: Field for updates (e.g., `updated_at`, `modified_date`)
- **Default Start Date**: Initial load date (format: `2023-09-01T00:00:00.000000`)

**Target:**
- **Target Database**: Default: `{client}_src`
- **Project Directory**: Default: `ingestion`

**Authentication:**
- Source system credentials (will be configured during ingestion setup)

---

### Phase 2: Hist-Union Configuration

**Tables to Combine:**
- List of tables requiring historical + incremental merge
- Format: `database.table_name` or `table_name` (uses default database)
- Example: `shopify_orders`, `shopify_customers`

**Settings:**
- **Project Directory**: Default: `hist_union`
- **Source Database**: From Phase 1 output (default: `{client}_src`)
- **Target Database**: Default: `{client}_src` (overwrites with combined data)

---

### Phase 3: Staging Configuration

**Transformation Settings:**
- **SQL Engine**: Choose one:
  - `presto` - Presto SQL (recommended for most cases)
  - `hive` - Hive SQL (for legacy compatibility)

**Tables to Transform:**
- Tables from hist-union output
- Will apply data cleaning, standardization, PII handling

**Settings:**
- **Project Directory**: Default: `staging`
- **Source Database**: From Phase 2 output
- **Target Database**: Default: `{client}_stg`

---

### Phase 4: Unification Configuration

**Unification Settings:**
- **Unification Name**: Project name (e.g., `customer_360`, `unified_profile`)
- **ID Method**: Choose one:
  - `persistent_id` - Stable IDs across updates (recommended)
  - `canonical_id` - Traditional merge approach

- **Update Strategy**: Choose one:
  - `incremental` - Process new/updated records only (recommended)
  - `full` - Reprocess all data each time

**Tables for Unification:**
- Staging tables with user identifiers
- Format: `database.table_name`
- Example: `acme_stg.shopify_orders`, `acme_stg.shopify_customers`

**Settings:**
- **Project Directory**: Default: `unification`
- **Regional Endpoint**: Same as Global TD_ENDPOINT

---

## Execution Process

### Step-by-Step Flow

I'll launch the **cdp-pipeline-orchestrator agent** to execute:

**1. Configuration Collection**
- Gather all inputs upfront
- Validate credentials and access
- Create execution plan
- Show complete configuration for approval

**2. Phase Execution Loop**

For each phase (Ingestion → Hist-Union → Staging → Unification):

```
┌─────────── PHASE EXECUTION ───────────┐
│                                       │
│ [1] GENERATE Workflows                │
│     → Invoke plugin slash command     │
│     → Wait for file generation        │
│     → Verify files created            │
│                                       │
│ [2] DEPLOY Workflows                  │
│     → Navigate to project directory   │
│     → Execute: td wf push             │
│     → Parse deployment result         │
│     → Auto-fix errors if possible     │
│     → Retry up to 3 times             │
│                                       │
│ [3] EXECUTE Workflows                 │
│     → Execute: td wf start            │
│     → Capture session ID              │
│     → Log start time                  │
│                                       │
│ [4] MONITOR Execution                 │
│     → Poll status every 30 seconds    │
│     → Show real-time progress         │
│     → Calculate elapsed time          │
│     → Wait for completion             │
│                                       │
│ [5] VALIDATE Output                   │
│     → Query TD for created tables     │
│     → Verify row counts > 0           │
│     → Check schema expectations       │
│                                       │
│ [6] PROCEED to Next Phase             │
│     → Only if validation passes       │
│     → Update progress tracker         │
│                                       │
└───────────────────────────────────────┘
```

**3. Final Report**
- Complete execution summary
- All files generated
- Deployment records
- Data quality metrics
- Next steps guidance

---

## What Happens in Each Step

### [1] GENERATE Workflows

**Tool**: SlashCommand

**Actions**:
```
Phase 1 → /cdp-ingestion:ingest-new
Phase 2 → /cdp-histunion:histunion-batch
Phase 3 → /cdp-staging:transform-batch
Phase 4 → /cdp-unification:unify-setup
```

**Output**: Complete workflow files (.dig, .yml, .sql)

**Verification**: Check files exist using Glob tool

---

### [2] DEPLOY Workflows

**Tool**: Bash

**Command**:
```bash
cd {project_directory}
td -k {TD_API_KEY} -e {TD_ENDPOINT} wf push {project_name}
```

**Success Indicators**:
- "Uploaded workflows"
- "Project uploaded"
- No error messages

**Error Handling**:

**Syntax Error**:
```
Error: syntax error in shopify_ingest_inc.dig:15
→ Read file to identify issue
→ Fix using Edit tool
→ Retry deployment
```

**Validation Error**:
```
Error: database 'acme_src' not found
→ Check database exists
→ Create if needed
→ Update configuration
→ Retry deployment
```

**Authentication Error**:
```
Error: authentication failed
→ Verify TD_API_KEY
→ Check endpoint URL
→ Ask user to provide correct credentials
→ Retry deployment
```

**Retry Logic**: Up to 3 attempts with auto-fixes

---

### [3] EXECUTE Workflows

**Tool**: Bash

**Command**:
```bash
td -k {TD_API_KEY} -e {TD_ENDPOINT} wf start {project} {workflow} --session now
```

**Output Parsing**:
```
session id: 123456789
attempt id: 987654321
```

**Captured**:
- Session ID (for monitoring)
- Attempt ID
- Start timestamp

---

### [4] MONITOR Execution

**Tool**: Bash (polling loop)

**Pattern**:
```bash
# Check 1 (immediately)
td -k {TD_API_KEY} -e {TD_ENDPOINT} wf session {session_id}
# Output: {"status": "running"}
# → Show: ⏳ Status: running (0:00 elapsed)

# Wait 30 seconds
sleep 30

# Check 2 (after 30s)
td -k {TD_API_KEY} -e {TD_ENDPOINT} wf session {session_id}
# Output: {"status": "running"}
# → Show: ⏳ Status: running (0:30 elapsed)

# Continue until status changes...

# Final check
td -k {TD_API_KEY} -e {TD_ENDPOINT} wf session {session_id}
# Output: {"status": "success"}
# → Show: ✓ Execution completed (15:30 elapsed)
```

**Status Handling**:

**Status: running**
- Continue polling
- Show progress indicator
- Wait 30 seconds

**Status: success**
- Show completion message
- Log final duration
- Proceed to validation

**Status: error**
- Retrieve logs: `td wf log {session_id}`
- Parse error message
- Show to user
- Ask: Retry / Fix / Skip / Abort

**Status: killed**
- Show killed message
- Ask user: Restart / Abort

**Maximum Wait**: 2 hours (240 checks)

---

### [5] VALIDATE Output

**Tool**: mcp__mcc_treasuredata__query

**Validation Queries**:

**Ingestion Phase**:
```sql
-- Check tables created
SELECT table_name, row_count
FROM information_schema.tables
WHERE database_name = '{target_database}'
  AND table_name LIKE '{source_name}%'

-- Expected: {source}_orders, {source}_customers, etc.
-- Verify: row_count > 0 for each table
```

**Hist-Union Phase**:
```sql
-- Check hist-union tables
SELECT table_name, row_count
FROM information_schema.tables
WHERE database_name = '{target_database}'
  AND table_name LIKE '%_hist_union'

-- Verify: row_count >= source table counts
```

**Staging Phase**:
```sql
-- Check staging tables
SELECT table_name, row_count
FROM information_schema.tables
WHERE database_name = '{target_database}'
  AND table_name LIKE '%_stg_%'

-- Check for transformed columns
DESCRIBE {database}.{table_name}
```

**Unification Phase**:
```sql
-- Check unification tables
SELECT table_name, row_count
FROM information_schema.tables
WHERE database_name = '{target_database}'
  AND (table_name LIKE '%_prep'
       OR table_name LIKE '%unified_id%'
       OR table_name LIKE '%enriched%')

-- Verify: unified_id_lookup exists
-- Verify: enriched tables have unified_id column
```

**Validation Results**:
- ✓ **PASS**: All tables found with data → Proceed
- ⚠ **WARN**: Tables found but empty → Ask user
- ✗ **FAIL**: Tables missing → Stop, show error

---

## Error Handling

### Deployment Errors

**Common Issues & Auto-Fixes**:

**1. YAML Syntax Error**
```
Error: syntax error at line 15: missing colon
→ Auto-fix: Add missing colon
→ Retry: Automatic
```

**2. Missing Database**
```
Error: database 'acme_src' does not exist
→ Check: Query information_schema
→ Create: If user approves
→ Retry: Automatic
```

**3. Secret Not Found**
```
Error: secret 'shopify_api_key' not found
→ Prompt: User to upload credentials
→ Wait: For user confirmation
→ Retry: After user uploads
```

**Retry Strategy**:
- Attempt 1: Auto-fix if possible
- Attempt 2: Ask user for input, fix, retry
- Attempt 3: Final attempt after user guidance
- After 3 failures: Ask user to Skip/Abort

---

### Execution Errors

**Common Issues**:

**1. Table Not Found**
```
Error: Table 'acme_src.shopify_orders' does not exist

Diagnosis:
- Previous phase (Ingestion) may have failed
- Table name mismatch in configuration
- Database permissions issue

Options:
1. Retry - Run workflow again
2. Check Previous Phase - Verify ingestion completed
3. Skip - Skip this phase (NOT RECOMMENDED)
4. Abort - Stop entire pipeline

Your choice:
```

**2. Query Timeout**
```
Error: Query exceeded timeout limit

Diagnosis:
- Data volume too large
- Query not optimized
- Warehouse too small

Options:
1. Retry - Attempt again
2. Increase Timeout - Update workflow configuration
3. Abort - Stop for investigation

Your choice:
```

**3. Authentication Failed**
```
Error: Authentication failed for data source

Diagnosis:
- Credentials expired
- Invalid API key
- Permissions changed

Options:
1. Update Credentials - Upload new credentials
2. Retry - Try again with existing credentials
3. Abort - Stop for manual fix

Your choice:
```

---

### User Decision Points

At each failure, I'll present:

```
┌─────────────────────────────────────────┐
│ ⚠ Phase X Failed                        │
├─────────────────────────────────────────┤
│ Workflow: {workflow_name}               │
│ Session ID: {session_id}                │
│ Error: {error_message}                  │
│                                         │
│ Possible Causes:                        │
│ 1. {cause_1}                            │
│ 2. {cause_2}                            │
│ 3. {cause_3}                            │
│                                         │
│ Options:                                │
│ 1. Retry - Run again with same config  │
│ 2. Fix - Let me help fix the issue     │
│ 3. Skip - Skip this phase (DANGEROUS)  │
│ 4. Abort - Stop entire pipeline        │
│                                         │
│ Your choice (1-4):                      │
└─────────────────────────────────────────┘
```

**Choice Handling**:
- **1 (Retry)**: Re-execute workflow immediately
- **2 (Fix)**: Interactive troubleshooting, then retry
- **3 (Skip)**: Warn about consequences, skip if confirmed
- **4 (Abort)**: Stop pipeline, generate partial report

---

## Progress Tracking

I'll use TodoWrite to show real-time progress:

```
✓ Pre-Flight: Configuration gathered
→ Phase 1: Ingestion
  ✓ Generate workflows
  ✓ Deploy workflows
  → Execute workflows (session: 123456789)
  ⏳ Monitor execution... (5:30 elapsed)
  ⏳ Validate output...
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

## Expected Timeline

**Typical Execution Times** (varies by data volume):

| Phase | Generation | Deployment | Execution | Validation | Total |
|-------|-----------|------------|-----------|------------|-------|
| **Ingestion** | 2-5 min | 30 sec | 15-60 min | 1 min | ~1 hour |
| **Hist-Union** | 1-2 min | 30 sec | 10-30 min | 1 min | ~30 min |
| **Staging** | 2-5 min | 30 sec | 20-45 min | 1 min | ~45 min |
| **Unification** | 3-5 min | 30 sec | 30-90 min | 2 min | ~1.5 hours |
| **TOTAL** | **~10 min** | **~2 min** | **~2-3 hours** | **~5 min** | **~3-4 hours** |

*Actual times depend on data volume, complexity, and TD warehouse size*

---

## Final Deliverables

Upon successful completion, you'll receive:

### 1. Generated Workflow Files

**Ingestion** (`ingestion/`):
- `{source}_ingest_inc.dig` - Incremental ingestion workflow
- `{source}_ingest_hist.dig` - Historical backfill workflow (if mode=both)
- `config/{source}_datasources.yml` - Datasource configuration
- `config/{source}_{object}_load.yml` - Per-object load configs

**Hist-Union** (`hist_union/`):
- `{source}_hist_union.dig` - Main hist-union workflow
- `queries/{table}_union.sql` - Union SQL per table

**Staging** (`staging/`):
- `transform_{source}.dig` - Transformation workflow
- `queries/{table}_transform.sql` - Transform SQL per table

**Unification** (`unification/`):
- `unif_runner.dig` - Main orchestration workflow
- `dynmic_prep_creation.dig` - Prep table creation
- `id_unification.dig` - ID unification via API
- `enrich_runner.dig` - Enrichment workflow
- `config/unify.yml` - Unification configuration
- `config/environment.yml` - Client environment
- `config/src_prep_params.yml` - Prep parameters
- `config/stage_enrich.yml` - Enrichment config
- `queries/` - 15+ SQL query files

### 2. Deployment Records

- Session IDs for all workflow executions
- Execution logs saved to `pipeline_logs/{date}/`
- Deployment timestamps
- Error logs (if any)

### 3. Data Quality Metrics

- Row counts per table
- ID unification statistics
- Coverage percentages
- Data quality scores

### 4. Documentation

- Complete configuration summary
- Deployment instructions
- Operational runbook
- Monitoring guidelines
- Troubleshooting playbook

---

## Prerequisites

**Before starting, ensure**:

✅ **TD Toolbelt Installed**:
```bash
# Check version
td --version

# If not installed:
# macOS: brew install td
# Linux: https://toolbelt.treasuredata.com/
```

✅ **Valid TD API Key**:
- Get from: https://console.treasuredata.com/app/users
- Requires write permissions
- Should not expire during execution

✅ **Network Access**:
- Can reach TD endpoint
- Can connect to source systems (for ingestion)

✅ **Sufficient Permissions**:
- Create databases
- Create tables
- Upload workflows
- Execute workflows
- Upload secrets

✅ **Source System Credentials**:
- API keys ready
- OAuth tokens current
- Service accounts configured

---

## Getting Started

**Ready to begin?**

Please provide the following information:

### Step 1: Global Configuration
```
TD_API_KEY: 12345/abcd...
TD_ENDPOINT: https://api.treasuredata.com
Client Name: acme
```

### Step 2: Ingestion Details
```
Source Name: shopify
Connector Type: rest
Objects: orders,customers,products
Mode: both
Incremental Field: updated_at
Start Date: 2023-09-01T00:00:00.000000
Target Database: acme_src
```

### Step 3: Hist-Union Details
```
Tables: shopify_orders,shopify_customers,shopify_products
```

### Step 4: Staging Details
```
SQL Engine: presto
Tables: (will use hist-union output)
Target Database: acme_stg
```

### Step 5: Unification Details
```
Unification Name: customer_360
ID Method: persistent_id
Update Strategy: incremental
Tables: (will use staging output)
```

---

**Alternatively, provide all at once in YAML format:**

```yaml
global:
  td_api_key: "12345/abcd..."
  td_endpoint: "https://api.treasuredata.com"
  client: "acme"

ingestion:
  source_name: "shopify"
  connector: "rest"
  objects: ["orders", "customers", "products"]
  mode: "both"
  incremental_field: "updated_at"
  start_date: "2023-09-01T00:00:00.000000"
  target_database: "acme_src"

hist_union:
  tables: ["shopify_orders", "shopify_customers", "shopify_products"]

staging:
  engine: "presto"
  target_database: "acme_stg"

unification:
  name: "customer_360"
  id_method: "persistent_id"
  update_strategy: "incremental"
```

---

**I'll orchestrate the complete CDP implementation from start to finish!**
