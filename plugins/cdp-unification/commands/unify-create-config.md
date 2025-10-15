---
name: unify-create-config
description: Generate core ID unification configuration files (unify.yml and id_unification.dig)
---

# Create Core Unification Configuration

## Overview

I'll generate core ID unification configuration files using the **id-unification-creator** specialized agent.

This command creates **TD-COMPLIANT** unification files:
- ✅ **DYNAMIC CONFIGURATION** - Based on prep table analysis
- ✅ **METHOD-SPECIFIC** - Persistent_id OR canonical_id (never both)
- ✅ **REGIONAL ENDPOINTS** - Correct URL for your region
- ✅ **SCHEMA VALIDATION** - Prevents first-run failures

---

## Prerequisites

**REQUIRED**: Prep table configuration must exist:
- `unification/config/environment.yml` - Client configuration
- `unification/config/src_prep_params.yml` - Prep table mappings

If you haven't created these yet, run:
- `/cdp-unification:unify-create-prep` first, OR
- `/cdp-unification:unify-setup` for complete end-to-end setup

---

## What You Need to Provide

### 1. ID Method Selection
Choose ONE method:

**Option A: persistent_id (RECOMMENDED)**
- Stable IDs that persist across updates
- Better for customer data platforms
- Recommended for most use cases
- **Provide persistent_id name** (e.g., `td_claude_id`, `stable_customer_id`)

**Option B: canonical_id**
- Traditional approach with merge capabilities
- Good for legacy systems
- **Provide canonical_id name** (e.g., `master_customer_id`)

### 2. Update Strategy
- **Full Refresh**: Reprocess all data each time (`full_refresh: true`)
- **Incremental**: Process only new/updated records (`full_refresh: false`)

### 3. Regional Endpoint
Choose your Treasure Data region:
- **US**: https://api-cdp.treasuredata.com/unifications/workflow_call
- **EU**: https://api-cdp.eu01.treasuredata.com/unifications/workflow_call
- **Asia Pacific**: https://api-cdp.ap02.treasuredata.com/unifications/workflow_call
- **Japan**: https://api-cdp.treasuredata.co.jp/unifications/workflow_call

### 4. Unification Name
- Name for this unification project (e.g., `claude`, `customer_360`)

---

## What I'll Do

### Step 1: Validate Prerequisites
I'll check that these files exist:
- `unification/config/environment.yml`
- `unification/config/src_prep_params.yml`

And extract:
- Client short name
- Unified input table name
- All prep table configurations with column mappings

### Step 2: Extract Key Information
I'll parse `src_prep_params.yml` to identify:
- All unique `alias_as` column names
- Key types: email, phone, td_client_id, td_global_id, customer_id, etc.
- Complete list of available keys for `merge_by_keys`

### Step 3: Generate unification/config/unify.yml
I'll create:
```yaml
name: {unif_name}

keys:
  - name: email
    invalid_texts: ['']
  - name: td_client_id
    invalid_texts: ['']
  - name: phone
    invalid_texts: ['']
  # ... ALL detected key types

tables:
  - database: ${client_short_name}_${stg}
    table: ${globals.unif_input_tbl}
    incremental_columns: [time]
    key_columns:
      - {column: email, key: email}
      - {column: td_client_id, key: td_client_id}
      - {column: phone, key: phone}
      # ... ALL alias_as columns mapped

# ONLY ONE of these sections (based on your selection):
persistent_ids:
  - name: {persistent_id_name}
    merge_by_keys: [email, td_client_id, phone, ...]
    merge_iterations: 15

# OR

canonical_ids:
  - name: {canonical_id_name}
    merge_by_keys: [email, td_client_id, phone, ...]
    merge_iterations: 15
```

### Step 4: Validate and Update Schema (CRITICAL)
I'll prevent first-run failures by:
1. Reading `unify.yml` to extract `merge_by_keys` list
2. Reading `queries/create_schema.sql` to check existing columns
3. Comparing required vs existing columns
4. Updating `create_schema.sql` if missing columns:
   - Add all keys from `merge_by_keys` as varchar
   - Add source, time, ingest_time columns
   - Update BOTH table definitions (main and tmp)

### Step 5: Generate unification/id_unification.dig
I'll create:
```yaml
timezone: UTC

_export:
  !include : config/environment.yml
  !include : config/src_prep_params.yml

+call_unification:
  http_call>: {REGIONAL_ENDPOINT_URL}
  headers:
    - authorization: ${secret:td.apikey}
    - content-type: application/json
  method: POST
  retry: true
  content_format: json
  content:
    run_persistent_ids: {true/false}    # ONLY if persistent_id
    run_canonical_ids: {true/false}     # ONLY if canonical_id
    run_enrichments: true
    run_master_tables: true
    full_refresh: {true/false}
    keep_debug_tables: true
    unification:
      !include : config/unify.yml
```

---

## Expected Output

### Files Created
```
unification/
├── config/
│   └── unify.yml                      ✓ Dynamic configuration
├── queries/
│   └── create_schema.sql              ✓ Updated with all columns
└── id_unification.dig                 ✓ Core unification workflow
```

### Example unify.yml (persistent_id method)
```yaml
name: customer_360

keys:
  - name: email
    invalid_texts: ['']
  - name: td_client_id
    invalid_texts: ['']
    valid_regexp: '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'

tables:
  - database: ${client_short_name}_${stg}
    table: ${globals.unif_input_tbl}
    incremental_columns: [time]
    key_columns:
      - {column: email, key: email}
      - {column: td_client_id, key: td_client_id}

persistent_ids:
  - name: td_claude_id
    merge_by_keys: [email, td_client_id]
    merge_iterations: 15
```

### Example id_unification.dig (US region, incremental)
```yaml
timezone: UTC

_export:
  !include : config/environment.yml
  !include : config/src_prep_params.yml

+call_unification:
  http_call>: https://api-cdp.treasuredata.com/unifications/workflow_call
  headers:
    - authorization: ${secret:td.apikey}
    - content-type: application/json
  method: POST
  retry: true
  content_format: json
  content:
    run_persistent_ids: true
    run_enrichments: true
    run_master_tables: true
    full_refresh: false
    keep_debug_tables: true
    unification:
      !include : config/unify.yml
```

---

## Critical Requirements

### ✅ Dynamic Configuration
- All keys detected from `src_prep_params.yml`
- All column mappings from prep analysis
- Method-specific configuration (never both)

### ⚠️ Schema Completeness
- `create_schema.sql` MUST contain ALL columns from `merge_by_keys`
- Prevents "column not found" errors on first run
- Updates both main and tmp table definitions

### ⚠️ Config File Inclusion
- `id_unification.dig` MUST include BOTH config files in `_export`:
  - `environment.yml` - For `${client_short_name}_${stg}`
  - `src_prep_params.yml` - For `${globals.unif_input_tbl}`

### ⚠️ Regional Endpoint
- Must use exact URL for selected region
- Different endpoints for US, EU, Asia Pacific, Japan

---

## Validation Checklist

Before completing, I'll verify:
- [ ] unify.yml contains all detected key types
- [ ] key_columns section maps ALL alias_as columns
- [ ] Only ONE ID method section exists
- [ ] merge_by_keys includes ALL available keys
- [ ] **CRITICAL**: create_schema.sql contains ALL columns from merge_by_keys
- [ ] **CRITICAL**: Both table definitions updated (main and tmp)
- [ ] id_unification.dig has correct regional endpoint
- [ ] **CRITICAL**: _export includes BOTH config files
- [ ] Workflow flags match selected method only
- [ ] Proper TD YAML/DIG syntax

---

## Success Criteria

All generated files will:
- ✅ **TD-COMPLIANT** - Work without modification in TD
- ✅ **DYNAMICALLY CONFIGURED** - Based on actual prep analysis
- ✅ **METHOD-ACCURATE** - Exact implementation of selected method
- ✅ **REGIONALLY CORRECT** - Proper endpoint for region
- ✅ **SCHEMA-COMPLETE** - All required columns present

---

## Next Steps

After creating core config, you can:
1. **Test unification workflow**: `dig run unification/id_unification.dig`
2. **Add enrichment**: Use `/cdp-unification:unify-setup` to add staging enrichment
3. **Create main orchestrator**: Combine prep + unification + enrichment

---

## Getting Started

**Ready to create core unification config?** Please provide:

1. **ID Method**:
   - Choose: `persistent_id` or `canonical_id`
   - Provide ID name: e.g., `td_claude_id`

2. **Update Strategy**:
   - Choose: `incremental` or `full_refresh`

3. **Regional Endpoint**:
   - Choose: `US`, `EU`, `Asia Pacific`, or `Japan`

4. **Unification Name**:
   - e.g., `customer_360`, `claude`

**Example:**
```
ID Method: persistent_id
ID Name: td_claude_id
Update Strategy: incremental
Region: US
Unification Name: customer_360
```

I'll call the **id-unification-creator** agent to generate all core unification files.

---

**Let's create your unification configuration!**
