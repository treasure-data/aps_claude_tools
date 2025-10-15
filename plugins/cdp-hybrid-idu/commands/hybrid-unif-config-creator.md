---
name: hybrid-unif-config-creator
description: Auto-generate unify.yml configuration for Snowflake/Databricks by extracting user identifiers from actual tables using strict PII detection
---

# Unify Configuration Creator for Snowflake/Databricks

## Overview

I'll automatically generate a production-ready `unify.yml` configuration file for your Snowflake or Databricks ID unification by:

1. **Analyzing your actual tables** using platform-specific MCP tools
2. **Extracting user identifiers** with zero-tolerance PII detection
3. **Validating data patterns** from real table data
4. **Generating unify.yml** using the exact template format
5. **Providing recommendations** for merge strategies and priorities

**This command uses STRICT analysis - only tables with actual user identifiers will be included.**

---

## What You Need to Provide

### 1. Platform Selection
- **Snowflake**: For Snowflake databases
- **Databricks**: For Databricks Unity Catalog tables

### 2. Tables to Analyze
Provide tables you want to analyze for ID unification:
- **Format (Snowflake)**: `database.schema.table` or `schema.table` or `table`
- **Format (Databricks)**: `catalog.schema.table` or `schema.table` or `table`
- **Example**: `customer_data.public.customers`, `orders`, `web_events.user_activity`

### 3. Canonical ID Configuration
- **Name**: Name for your unified ID (default: `td_id`)
- **Merge Iterations**: Number of unification loop iterations (default: 10)
- **Incremental Iterations**: Iterations for incremental processing (default: 5)

### 4. Output Configuration (Optional)
- **Output File**: Where to save unify.yml (default: `unify.yml`)
- **Template Path**: Path to template if using custom (default: uses built-in exact template)

---

## What I'll Do

### Step 1: Platform Detection and Validation
```
1. Confirm platform (Snowflake or Databricks)
2. Verify MCP tools are available for the platform
3. Set up platform-specific query patterns
4. Inform you of the analysis approach
```

### Step 2: Key Extraction with hybrid-unif-keys-extractor Agent
I'll launch the **hybrid-unif-keys-extractor agent** to:

**Schema Analysis**:
- Use platform MCP tools to describe each table
- Extract exact column names and data types
- Identify accessible vs inaccessible tables

**User Identifier Detection**:
- Apply STRICT matching rules for user identifiers:
  - ✅ Email columns (email, email_std, email_address, etc.)
  - ✅ Phone columns (phone, phone_number, mobile_phone, etc.)
  - ✅ User IDs (user_id, customer_id, account_id, etc.)
  - ✅ Cookie/Device IDs (td_client_id, cookie_id, etc.)
  - ❌ System columns (id, created_at, time, etc.)
  - ❌ Complex types (arrays, maps, objects, variants, structs)

**Data Validation**:
- Query actual MIN/MAX values from each identified column
- Analyze data patterns and quality
- Count unique values per identifier
- Detect data quality issues

**Table Classification**:
- **INCLUDED**: Tables with valid user identifiers
- **EXCLUDED**: Tables without user identifiers (fully documented why)

**Expert Analysis**:
- 3 SQL experts review the data
- Provide priority recommendations
- Suggest validation rules based on actual data patterns

### Step 3: Unify.yml Generation

**CRITICAL**: Using the **EXACT BUILT-IN template structure** (embedded in hybrid-unif-keys-extractor agent)

**Template Usage Process**:
```
1. Receive structured data from hybrid-unif-keys-extractor agent:
   - Keys with validation rules
   - Tables with column mappings
   - Canonical ID configuration
   - Master tables specification

2. Use BUILT-IN template structure (see agent documentation)

3. ONLY replace these specific values:
   - Line 1: name: {canonical_id_name}
   - keys section: actual keys found
   - tables section: actual tables with actual columns
   - canonical_ids section: name and merge_by_keys
   - master_tables section: [] or user specifications

4. PRESERVE everything else:
   - ALL comment blocks (#####...)
   - ALL comment text ("Declare Validation logic", etc.)
   - ALL spacing and indentation (2 spaces per level)
   - ALL blank lines
   - EXACT YAML structure

5. Use Write tool to save populated unify.yml
```

**I'll generate**:

**Section 1: Canonical ID Name**
```yaml
name: {your_canonical_id_name}
```

**Section 2: Keys with Validation**
```yaml
keys:
  - name: email
    valid_regexp: ".*@.*"
    invalid_texts: ['', 'N/A', 'null']
  - name: customer_id
    invalid_texts: ['', 'N/A', 'null']
  - name: phone_number
    invalid_texts: ['', 'N/A', 'null']
```
*Populated with actual keys found in your tables*

**Section 3: Tables with Key Column Mappings**
```yaml
tables:
  - database: {database/catalog}
    table: {table_name}
    key_columns:
      - {column: actual_column_name, key: mapped_key}
      - {column: another_column, key: another_key}
```
*Only tables with valid user identifiers, with EXACT column names from schema analysis*

**Section 4: Canonical IDs Configuration**
```yaml
canonical_ids:
  - name: {your_canonical_id_name}
    merge_by_keys: [email, customer_id, phone_number]
    merge_iterations: 10
```
*Based on extracted keys and your configuration*

**Section 5: Master Tables (Optional)**
```yaml
master_tables:
  - name: {canonical_id_name}_master_table
    canonical_id: {canonical_id_name}
    attributes:
      - name: best_email
        source_columns:
          - {table: table1, column: email, order: last, order_by: time, priority: 1}
          - {table: table2, column: email_address, order: last, order_by: time, priority: 2}
```
*If you request master table configuration, I'll help set up attribute aggregation*

### Step 4: Validation and Review

After generation:
```
1. Show complete unify.yml content
2. Highlight key sections:
   - Keys found: [list]
   - Tables included: [count]
   - Tables excluded: [count] with reasons
   - Merge strategy: [keys and priorities]
3. Provide recommendations for optimization
4. Ask for your approval before saving
```

### Step 5: File Output

```
1. Write unify.yml to specified location
2. Create backup of existing file if present
3. Provide file summary:
   - Keys configured: X
   - Tables configured: Y
   - Validation rules: Z
4. Show next steps for using the configuration
```

---

## Example Workflow

**Input**:
```
Platform: Snowflake
Tables:
  - customer_data.public.customers
  - customer_data.public.orders
  - web_data.public.events
Canonical ID Name: unified_customer_id
Output: snowflake_unify.yml
```

**Process**:
```
✓ Platform: Snowflake MCP tools detected
✓ Analyzing 3 tables...

Schema Analysis:
  ✓ customer_data.public.customers - 12 columns
  ✓ customer_data.public.orders - 8 columns
  ✓ web_data.public.events - 15 columns

User Identifier Detection:
  ✓ customers: email, customer_id (2 identifiers)
  ✓ orders: customer_id, email_address (2 identifiers)
  ✗ events: NO user identifiers found
    Available columns: event_id, session_id, page_url, timestamp, ...
    Reason: Contains only event tracking data - no PII

Data Analysis:
  ✓ email: 45,123 unique values, format valid
  ✓ customer_id: 45,089 unique values, numeric
  ✓ email_address: 12,456 unique values, format valid

Expert Analysis Complete:
  Priority 1: customer_id (most stable, highest coverage)
  Priority 2: email (good coverage, some quality issues)
  Priority 3: phone_number (not found)

Generating unify.yml...
  ✓ Keys section: 2 keys configured
  ✓ Tables section: 2 tables configured
  ✓ Canonical IDs: unified_customer_id
  ✓ Validation rules: Applied based on data patterns

Tables EXCLUDED:
  - web_data.public.events: No user identifiers
```

**Output (snowflake_unify.yml)**:
```yaml
name: unified_customer_id

keys:
  - name: email
    valid_regexp: ".*@.*"
    invalid_texts: ['', 'N/A', 'null']
  - name: customer_id
    invalid_texts: ['', 'N/A', 'null']

tables:
  - database: customer_data
    table: customers
    key_columns:
      - {column: email, key: email}
      - {column: customer_id, key: customer_id}
  - database: customer_data
    table: orders
    key_columns:
      - {column: email_address, key: email}
      - {column: customer_id, key: customer_id}

canonical_ids:
  - name: unified_customer_id
    merge_by_keys: [customer_id, email]
    merge_iterations: 15

master_tables: []
```

---

## Key Features

### 🔍 **STRICT PII Detection**
- Zero tolerance for guessing
- Only includes tables with actual user identifiers
- Documents why tables are excluded
- Based on REAL schema and data analysis

### ✅ **Exact Template Compliance**
- Uses BUILT-IN exact template structure (embedded in hybrid-unif-keys-extractor agent)
- NO modifications to template format
- Preserves all comment sections
- Maintains exact YAML structure
- Portable across all systems

### 📊 **Real Data Analysis**
- Queries actual MIN/MAX values
- Counts unique identifiers
- Validates data patterns
- Identifies quality issues

### 🎯 **Platform-Aware**
- Uses correct MCP tools for each platform
- Respects platform naming conventions
- Applies platform-specific data type rules
- Generates platform-compatible SQL references

### 📋 **Complete Documentation**
- Documents all excluded tables with reasons
- Lists available columns for excluded tables
- Explains why columns don't qualify as user identifiers
- Provides expert recommendations

---

## Output Format

**The generated unify.yml will have EXACTLY this structure:**

```yaml
name: {canonical_id_name}
#####################################################
##
##Declare Validation logic for unification keys
##
#####################################################
keys:
  - name: {key1}
    valid_regexp: "{pattern}"
    invalid_texts: ['{val1}', '{val2}', '{val3}']
  - name: {key2}
    invalid_texts: ['{val1}', '{val2}', '{val3}']

#####################################################
##
##Declare databases, tables, and keys to use during unification
##
#####################################################

tables:
  - database: {db/catalog}
    table: {table}
    key_columns:
      - {column: {col}, key: {key}}

#####################################################
##
##Declare hierarchy for unification. Define keys to use for each level.
##
#####################################################

canonical_ids:
  - name: {canonical_id_name}
    merge_by_keys: [{key1}, {key2}, ...]
    merge_iterations: {number}
    incremental_merge_iterations: {number}

#####################################################
##
##Declare Similar Attributes and standardize into a single column
##
#####################################################

master_tables:
  - name: {canonical_id_name}_master_table
    canonical_id: {canonical_id_name}
    attributes:
      - name: {attribute}
        source_columns:
          - {table: {t}, column: {c}, order: last, order_by: time, priority: 1}
```

**NO deviations from this structure - EXACT template compliance guaranteed.**

---

## Prerequisites

### Required:
- ✅ Snowflake or Databricks platform access
- ✅ Platform-specific MCP tools configured (may use fallback if unavailable)
- ✅ Read permissions on tables to be analyzed
- ✅ Tables must exist and be accessible

### Optional:
- Custom unify.yml template path (if not using default)
- Master table attribute specifications
- Custom validation rules

---

## Expected Timeline

| Step | Duration |
|------|----------|
| Platform detection | < 1 min |
| Schema analysis (per table) | 5-10 sec |
| Data analysis (per identifier) | 10-20 sec |
| Expert analysis | 1-2 min |
| YAML generation | < 1 min |
| **Total (for 5 tables)** | **~3-5 min** |

---

## Error Handling

### Common Issues:

**Issue**: MCP tools not available for platform
**Solution**:
- I'll inform you and provide fallback options
- You can provide schema information manually
- I'll still generate unify.yml with validation warnings

**Issue**: No tables have user identifiers
**Solution**:
- I'll show you why tables were excluded
- Suggest alternative tables to analyze
- Explain what constitutes a user identifier

**Issue**: Table not accessible
**Solution**:
- Document which tables are inaccessible
- Continue with accessible tables
- Recommend permission checks

**Issue**: Complex data types found
**Solution**:
- Exclude complex type columns (arrays, structs, maps)
- Explain why they can't be used for unification
- Suggest alternative columns if available

---

## Success Criteria

Generated unify.yml will:
- ✅ Use EXACT template structure - NO modifications
- ✅ Contain ONLY tables with validated user identifiers
- ✅ Include ONLY columns that actually exist in tables
- ✅ Have validation rules based on actual data patterns
- ✅ Be ready for immediate use with hybrid-generate-snowflake or hybrid-generate-databricks
- ✅ Work without any manual edits
- ✅ Include comprehensive documentation in comments

---

## Next Steps After Generation

1. **Review the generated unify.yml**
   - Verify tables and columns are correct
   - Check validation rules are appropriate
   - Review merge strategy and priorities

2. **Generate SQL for your platform**:
   - Snowflake: `/cdp-hybrid-idu:hybrid-generate-snowflake`
   - Databricks: `/cdp-hybrid-idu:hybrid-generate-databricks`

3. **Execute the workflow**:
   - Snowflake: `/cdp-hybrid-idu:hybrid-execute-snowflake`
   - Databricks: `/cdp-hybrid-idu:hybrid-execute-databricks`

4. **Monitor convergence and results**

---

## Getting Started

**Ready to begin?**

Please provide:

1. **Platform**: Snowflake or Databricks
2. **Tables**: List of tables to analyze (full paths)
3. **Canonical ID Name**: Name for your unified ID (e.g., `unified_customer_id`)
4. **Output File** (optional): Where to save unify.yml (default: `unify.yml`)

**Example**:
```
Platform: Snowflake
Tables:
  - customer_db.public.customers
  - customer_db.public.orders
  - marketing_db.public.campaigns
Canonical ID: unified_id
Output: snowflake_unify.yml
```

---

**I'll analyze your tables and generate a production-ready unify.yml configuration!**
