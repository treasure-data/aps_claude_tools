---
name: unify-create-prep
description: Generate prep table creation files and configuration for ID unification
---

# Create Prep Table Configuration

## Overview

I'll generate prep table creation files and configuration using the **dynamic-prep-creation** specialized agent.

This command creates **PRODUCTION-READY** prep table files:
- ⚠️ **EXACT TEMPLATES** - No modifications allowed
- ⚠️ **ZERO CHANGES** - Character-for-character accuracy
- ✅ **GENERIC FILES** - Reusable across all projects
- ✅ **DYNAMIC CONFIGURATION** - Adapts to your table structure

---

## What You Need to Provide

### 1. Table Analysis Results
If you've already run key extraction:
- Provide the list of **included tables** with their user identifier columns
- I can use the results from `/cdp-unification:unify-extract-keys`

OR provide directly:
- **Source tables**: database.table_name format
- **User identifier columns**: For each table, which columns contain identifiers

### 2. Client Configuration
- **Client short name**: Your client identifier (e.g., `mck`, `client_name`)
- **Database suffixes**:
  - Source database suffix (default: `src`)
  - Staging database suffix (default: `stg`)
  - Lookup database (default: `references`)

### 3. Column Mappings
For each table, specify which columns to include and their unified aliases:
- **Email columns** → alias: `email`
- **Phone columns** → alias: `phone`
- **Customer ID columns** → alias: `customer_id`
- **TD Client ID** → alias: `td_client_id`
- **TD Global ID** → alias: `td_global_id`

---

## What I'll Do

### Step 1: Create Directory Structure
I'll create:
- `unification/config/` directory
- `unification/queries/` directory

### Step 2: Generate Generic Files (EXACT TEMPLATES)
I'll create these files with **ZERO MODIFICATIONS**:

**⚠️ `unification/dynmic_prep_creation.dig`** (EXACT filename - no 'a' in dynmic)
- Generic prep workflow
- Handles schema creation, table looping, and data insertion
- Uses variables from config files

**⚠️ `unification/queries/create_schema.sql`**
- Generic schema creation for unified input table
- Creates both main and tmp tables

**⚠️ `unification/queries/loop_on_tables.sql`**
- Complex production SQL for dynamic table processing
- Generates prep table SQL and unified input table SQL
- Handles incremental logic and deduplication

**⚠️ `unification/queries/unif_input_tbl.sql`**
- DSAR processing and data cleaning
- Exclusion list management for masked data
- Dynamic column detection and insertion

### Step 3: Generate Dynamic Configuration Files

**`unification/config/environment.yml`**
```yaml
client_short_name: {your_client_name}
src: src
stg: stg
gld: gld
lkup: references
```

**`unification/config/src_prep_params.yml`**
- Dynamic table configuration based on your table analysis
- Column mappings with unified aliases
- Prep table naming conventions

### Step 4: Dynamic Column Detection (CRITICAL)
For `unif_input_tbl.sql`, I'll:
1. Query Treasure Data schema: `information_schema.columns`
2. Detect all columns besides email, phone, source, ingest_time, time
3. Auto-generate column list for data_cleaned CTE
4. Replace placeholder with actual columns

---

## Expected Output

### Generic Files (EXACT - NO CHANGES)
```
unification/
├── dynmic_prep_creation.dig          ⚠️ EXACT filename
├── queries/
│   ├── create_schema.sql             ⚠️ EXACT content
│   ├── loop_on_tables.sql            ⚠️ EXACT content
│   └── unif_input_tbl.sql            ⚠️ WITH dynamic columns
```

### Dynamic Configuration Files
```
unification/config/
├── environment.yml                   ✓ Client-specific
└── src_prep_params.yml              ✓ Table-specific
```

### Example src_prep_params.yml Structure
```yaml
globals:
  unif_input_tbl: unif_input

prep_tbls:
  - src_tbl: user_events
    src_db: ${client_short_name}_${stg}
    snk_db: ${client_short_name}_${stg}
    snk_tbl: ${src_tbl}_prep
    columns:
      - col:
        name: user_email
        alias_as: email
      - col:
        name: td_client_id
        alias_as: td_client_id

  - src_tbl: customers
    src_db: ${client_short_name}_${stg}
    snk_db: ${client_short_name}_${stg}
    snk_tbl: ${src_tbl}_prep
    columns:
      - col:
        name: email
        alias_as: email
      - col:
        name: customer_id
        alias_as: customer_id
```

---

## Critical Requirements

### ⚠️ NEVER MODIFY GENERIC FILES
- **dynmic_prep_creation.dig**: EXACT template, character-for-character
- **create_schema.sql**: EXACT SQL, no changes
- **loop_on_tables.sql**: EXACT complex SQL, no modifications
- **unif_input_tbl.sql**: EXACT template + dynamic column replacement

### ✅ DYNAMIC CONFIGURATION ONLY
- **environment.yml**: Client-specific variables
- **src_prep_params.yml**: Table-specific mappings

### 🚨 CRITICAL FILENAME
- **MUST be "dynmic_prep_creation.dig"** (NO 'a' in dynmic)
- This is intentional - production systems expect this exact name

### 🚨 NO TIME COLUMN
- **NEVER ADD** `time` column to src_prep_params.yml
- Time is auto-generated by SQL template
- Only include actual identifier columns

---

## Validation Checklist

Before completing, I'll verify:
- [ ] File named "dynmic_prep_creation.dig" exists
- [ ] Content matches template character-for-character
- [ ] All variable placeholders preserved
- [ ] Queries folder contains exact SQL files
- [ ] Config folder contains YAML files
- [ ] Dynamic columns inserted in unif_input_tbl.sql
- [ ] No time column in src_prep_params.yml
- [ ] All directories created

---

## Success Criteria

All generated files will:
- ✅ **EXACT TEMPLATES** - Character-for-character accuracy
- ✅ **PRODUCTION-READY** - Deployable to TD without changes
- ✅ **DYNAMIC CONFIGURATION** - Adapts to table structure
- ✅ **DSAR COMPLIANT** - Includes exclusion list processing
- ✅ **INCREMENTAL PROCESSING** - Supports time-based updates

---

## Next Steps

After prep creation, you can:
1. **Test prep workflow**: `dig run unification/dynmic_prep_creation.dig`
2. **Create unification config**: Use `/cdp-unification:unify-create-config`
3. **Complete full setup**: Use `/cdp-unification:unify-setup`

---

## Getting Started

**Ready to create prep tables?** Please provide:

1. **Table list with columns**:
   ```
   Table: analytics.user_events
   Columns: user_email (email), td_client_id (td_client_id)

   Table: crm.customers
   Columns: email (email), customer_id (customer_id)
   ```

2. **Client configuration**:
   ```
   Client short name: mck
   ```

I'll call the **dynamic-prep-creation** agent to generate all prep files with exact templates.

---

**Let's create your prep table configuration!**
