---
name: unify-setup
description: Complete end-to-end ID unification setup from table analysis to deployment
---

# Complete ID Unification Setup

## Overview

I'll guide you through the complete ID unification setup process for Treasure Data CDP. This is an interactive, end-to-end workflow that will:

1. **Extract and validate user identifiers** from your tables
2. **Help you choose the right ID method** (canonical_id vs persistent_id)
3. **Generate prep table configurations** for data standardization
4. **Create core unification files** (unify.yml and id_unification.dig)
5. **Set up staging enrichment** for post-unification processing
6. **Create orchestration workflow** (unif_runner.dig) to run everything in sequence

---

## What You Need to Provide

### 1. Table List
Please provide the list of tables you want to include in ID unification:
- Format: `database.table_name` (e.g., `analytics.user_events`, `crm.customers`)
- I'll analyze each table using Treasure Data MCP tools to extract user identifiers

### 2. Client Configuration
- **Client short name**: Your client identifier (e.g., `mck`, `client`)
- **Unification name**: Name for this unification project (e.g., `claude`, `customer_360`)
- **Lookup/Config database suffix**: (default: `config`)
  - Creates database: `${client_short_name}_${lookup_suffix}` (e.g., `client_config`)
  - ⚠️ **I WILL CREATE THIS DATABASE** if it doesn't exist

### 3. ID Method Selection
I'll explain the options and help you choose:
- **persistent_id**: Stable IDs that persist across updates (recommended for most cases)
- **canonical_id**: Traditional approach with merge capabilities

### 4. Update Strategy
- **Incremental**: Process only new/updated records
- **Full Refresh**: Reprocess all data each time

### 5. Regional Endpoint
- **US**: https://api-cdp.treasuredata.com
- **EU**: https://api-cdp.eu01.treasuredata.com
- **Asia Pacific**: https://api-cdp.ap02.treasuredata.com
- **Japan**: https://api-cdp.treasuredata.co.jp

---

## What I'll Do

### Step 1: Extract and Validate Keys (via unif-keys-extractor agent)
I'll:
- Use Treasure Data MCP tools to analyze table schemas
- Extract user identifier columns (email, phone, td_client_id, etc.)
- Query sample data to validate identifier patterns
- Provide 3 SQL experts analysis of key relationships
- Recommend priority ordering for unification keys
- Exclude tables without user identifiers

### Step 2: Configuration Guidance
I'll:
- Explain canonical_id vs persistent_id concepts
- Recommend best approach for your use case
- Discuss incremental vs full refresh strategies
- Help you understand regional endpoint requirements

### Step 3: Generate Prep Tables (via dynamic-prep-creation agent)
I'll create:
- `unification/dynmic_prep_creation.dig` - Prep workflow
- `unification/queries/create_schema.sql` - Schema creation
- `unification/queries/loop_on_tables.sql` - Dynamic loop logic
- `unification/queries/unif_input_tbl.sql` - DSAR processing and data cleaning
- `unification/config/environment.yml` - Client configuration
- `unification/config/src_prep_params.yml` - Dynamic table mappings

### Step 4: Generate Core Unification (via id-unification-creator agent)
I'll create:
- `unification/config/unify.yml` - Unification configuration with keys and tables
- `unification/id_unification.dig` - Core unification workflow with HTTP API call
- Updated `unification/queries/create_schema.sql` - Schema with all required columns

### Step 5: Generate Staging Enrichment (via unification-staging-enricher agent)
I'll create:
- `unification/config/stage_enrich.yml` - Enrichment configuration
- `unification/enrich/queries/generate_join_query.sql` - Join query generation
- `unification/enrich/queries/execute_join_presto.sql` - Presto execution
- `unification/enrich/queries/execute_join_hive.sql` - Hive execution
- `unification/enrich/queries/enrich_tbl_creation.sql` - Table creation
- `unification/enrich_runner.dig` - Enrichment workflow

### Step 6: Create Main Orchestration
I'll create:
- `unification/unif_runner.dig` - Main workflow that calls:
  - prep_creation → id_unification → enrichment (in sequence)

### Step 7: ⚠️ MANDATORY VALIDATION (NEW!)
**CRITICAL**: Before deployment, I MUST run comprehensive validation:
- `/cdp-unification:unify-validate` command
- Validates ALL files against exact templates
- Checks database and table existence
- Verifies configuration consistency
- **BLOCKS deployment if ANY validation fails**

**If validation FAILS:**
- I will show exact fix commands
- You must fix all errors
- Re-run validation until 100% pass
- Only then proceed to deployment

**If validation PASSES:**
- Proceed to deployment with confidence
- All files are production-ready

### Step 8: Deployment Guidance
I'll provide:
- Configuration summary
- Deployment instructions
- Operating guidelines
- Monitoring recommendations

---

## Interactive Workflow

I'll use the **TD Copilot communication pattern** throughout:

- **Question**: When I need your input or choice
- **Suggestion**: When I recommend a specific approach
- **Check Point**: When you should verify understanding

---

## Expected Output

### Files Created (All under `unification/` directory):

**Workflows:**
- `unif_runner.dig` - Main orchestration workflow
- `dynmic_prep_creation.dig` - Prep table creation
- `id_unification.dig` - Core unification
- `enrich_runner.dig` - Staging enrichment

**Configuration:**
- `config/environment.yml` - Client settings
- `config/src_prep_params.yml` - Prep table mappings
- `config/unify.yml` - Unification configuration
- `config/stage_enrich.yml` - Enrichment configuration

**SQL Templates:**
- `queries/create_schema.sql` - Schema creation
- `queries/loop_on_tables.sql` - Dynamic loop logic
- `queries/unif_input_tbl.sql` - DSAR and data cleaning
- `enrich/queries/generate_join_query.sql` - Join generation
- `enrich/queries/execute_join_presto.sql` - Presto execution
- `enrich/queries/execute_join_hive.sql` - Hive execution
- `enrich/queries/enrich_tbl_creation.sql` - Table creation

---

## Success Criteria

All generated files will:
- ✅ Be TD-compliant and deployment-ready
- ✅ Use exact templates from documentation
- ✅ Include comprehensive error handling
- ✅ Follow TD Copilot standards
- ✅ Work without modification in Treasure Data
- ✅ Support incremental processing
- ✅ Include DSAR processing
- ✅ Generate proper master tables

---

## Getting Started

**Ready to begin?** Please provide:

1. Your table list (database.table_name format)
2. Client short name
3. Unification name

I'll start by analyzing your tables with the unif-keys-extractor agent to extract and validate user identifiers.

**Example:**
```
I want to set up ID unification for:
- analytics.user_events
- crm.customers
- web.pageviews

Client: mck
Unification name: customer_360
```

---

**Let's get started!**
