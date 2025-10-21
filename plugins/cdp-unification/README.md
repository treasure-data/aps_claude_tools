# CDP Unification Plugin

**Production-ready identity resolution and customer unification for Treasure Data**

---

## Overview

The CDP Unification plugin automates customer identity resolution across multiple data sources in Treasure Data. It uses live table analysis via MCP to extract identity keys, create prep tables, generate ID graphs, and produce unified customer master records.

### Purpose

Create a single customer view (golden record) by automating:
- Identity key extraction (email, phone, user_id, etc.)
- Prep table generation for identity matching
- ID graph configuration with unify.yml
- Identity resolution workflow creation
- Master customer record generation
- Cross-source identity linking

---

## Features

### Live Table Analysis via MCP
- **Real-Time Schema Access**: Direct connection to TD API
- **Automatic Key Detection**: Identifies PII columns (email, phone, user_id, etc.)
- **No Manual Configuration**: AI-driven column identification
- **Validation**: Ensures keys exist and have data

### Identity Key Extraction
- **Email Detection**: Finds email columns automatically
- **Phone Number Detection**: Identifies phone fields
- **User ID Detection**: Locates user_id, customer_id fields
- **Custom Keys**: Support for additional identifier types
- **Validation Rules**: Regex patterns for key validation

### Prep Table Generation
- **SQL Generation**: Creates prep table queries
- **Deduplication**: Handles duplicate identifiers
- **Standardization**: Lowercase emails, trimmed values
- **NULL Filtering**: Excludes invalid identifiers
- **Table per Key Type**: Separate preps for email, phone, etc.

### ID Graph Configuration
- **YAML Generation**: Creates unify.yml config
- **Key Mapping**: Links tables to identifier types
- **Merge Strategy**: Defines how IDs connect
- **Hierarchy Support**: Multi-level unification (person → household)

### Master Record Creation
- **Attribute Selection**: Priority-based best value selection
- **Array Aggregation**: Collect all values for fields
- **Deduplication**: Single record per unified ID
- **Enrichment**: Add derived fields

---

## Slash Commands

### `/cdp-unification:unify-setup`

Complete end-to-end ID unification setup - one command does it all.

**Usage:**
```bash
/cdp-unification:unify-setup
```

**Prompts for:**
- Target database with customer data
- Tables containing customer identifiers
- Identity types to use (email, phone, user_id, etc.)
- Master table configuration

**Executes All Steps:**
1. Extracts identity keys from tables (via MCP)
2. Generates prep table SQL files
3. Creates unify.yml configuration
4. Generates id_unification.dig workflow
5. Validates all generated files

**Generates:**
- `unification/prep_*.sql` (one per key type)
- `unify.yml` (configuration)
- `id_unification.dig` (workflow)
- `unification/master_customers.sql` (master table)

**Example Flow:**
```
Step 1: Analyzing tables via MCP...
  ✓ Found 3 email columns across 2 tables
  ✓ Found 2 phone columns in 1 table
  ✓ Found 1 user_id column

Step 2: Generating prep tables...
  ✓ Created prep_email.sql
  ✓ Created prep_phone.sql
  ✓ Created prep_user_id.sql

Step 3: Creating unify.yml...
  ✓ Configured 3 keys
  ✓ Mapped 3 tables
  ✓ Defined merge strategy

Step 4: Generating workflow...
  ✓ Created id_unification.dig

Complete! Ready to execute.
```

---

### `/cdp-unification:unify-extract-keys`

Extract and validate identity columns from tables using live TD analysis.

**Usage:**
```bash
/cdp-unification:unify-extract-keys
```

**Prompts for:**
- Database name
- Tables to analyze
- Key types to look for (optional)

**MCP Analysis:**
- Connects to TD API
- Fetches table schemas
- Identifies PII columns by name patterns
- Validates column existence
- Checks for data presence

**Output:**
```json
{
  "email_keys": [
    {"table": "customer_profiles", "column": "email"},
    {"table": "order_history", "column": "customer_email"}
  ],
  "phone_keys": [
    {"table": "customer_profiles", "column": "phone_number"}
  ],
  "user_id_keys": [
    {"table": "customer_profiles", "column": "customer_id"},
    {"table": "order_history", "column": "user_id"}
  ]
}
```

**Return to user:**
- Summary of identified keys
- Recommendations for prep tables
- Warnings for missing or problematic columns

---

### `/cdp-unification:unify-create-prep`

Generate prep table SQL files for identity matching.

**Usage:**
```bash
/cdp-unification:unify-create-prep
```

**Prompts for:**
- Extracted keys (from previous step or manual input)
- Prep table naming convention
- Output database

**Generates:**
One SQL file per key type:
- `unification/prep_email.sql`
- `unification/prep_phone.sql`
- `unification/prep_user_id.sql`

**Example Prep Table SQL:**
```sql
-- File: unification/prep_email.sql

INSERT OVERWRITE TABLE unified_db.prep_email
SELECT DISTINCT
    'customer_profiles' AS source_table,
    'email' AS source_column,
    LOWER(TRIM(email)) AS key_value,
    customer_id AS source_id
FROM staging_db.customer_profiles
WHERE email IS NOT NULL
    AND email != ''
    AND REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z]{2,}$')

UNION ALL

SELECT DISTINCT
    'order_history' AS source_table,
    'customer_email' AS source_column,
    LOWER(TRIM(customer_email)) AS key_value,
    user_id AS source_id
FROM staging_db.order_history
WHERE customer_email IS NOT NULL
    AND customer_email != ''
    AND REGEXP_LIKE(customer_email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z]{2,}$')
```

---

### `/cdp-unification:unify-create-config`

Generate core ID unification configuration (unify.yml and id_unification.dig).

**Usage:**
```bash
/cdp-unification:unify-create-config
```

**Prompts for:**
- Prep tables created in previous step
- Canonical ID name (e.g., `unified_customer_id`)
- Merge keys to use
- Master table attributes

**Generates:**
1. `unify.yml` - Configuration file
2. `id_unification.dig` - Digdag workflow

**Example unify.yml:**
```yaml
name: customer_unification

# Define identity keys and validation rules
keys:
  - name: email
    valid_regexp: "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z]{2,}$"
    invalid_texts: ['', 'N/A', 'null', 'test@test.com']

  - name: phone
    valid_regexp: "^[0-9]{10,}$"
    invalid_texts: ['', 'N/A', 'null', '0000000000']

  - name: user_id
    invalid_texts: ['', 'N/A', 'null']

# Map tables to keys
tables:
  - database: staging_db
    table: customer_profiles
    key_columns:
      - {column: email, key: email}
      - {column: phone_number, key: phone}
      - {column: customer_id, key: user_id}

  - database: staging_db
    table: order_history
    key_columns:
      - {column: customer_email, key: email}
      - {column: user_id, key: user_id}

# Define canonical ID
canonical_ids:
  - name: unified_customer_id
    merge_by_keys: [email, phone, user_id]
    merge_iterations: 15

# Define master table
master_tables:
  - name: unified_customers
    canonical_id: unified_customer_id
    attributes:
      - name: best_email
        source_columns:
          - {table: customer_profiles, column: email, priority: 1}
          - {table: order_history, column: customer_email, priority: 2}

      - name: all_emails
        array_elements: 5
        source_columns:
          - {table: customer_profiles, column: email, priority: 1}
          - {table: order_history, column: customer_email, priority: 2}
```

**Example id_unification.dig:**
```yaml
timezone: UTC

_export:
  td:
    database: unified_db

# Create prep tables
+create_prep_email:
  td>: unification/prep_email.sql

+create_prep_phone:
  td>: unification/prep_phone.sql

+create_prep_user_id:
  td>: unification/prep_user_id.sql

# Run ID unification
+run_unification:
  require>: [create_prep_email, create_prep_phone, create_prep_user_id]
  td_unify>: unify.yml
  database: unified_db

# Create master table
+create_master:
  require>: [run_unification]
  td>: unification/master_customers.sql
  database: unified_db
```

---

## Identity Resolution Process

### Step-by-Step Flow

```
┌─────────────────────────────────────────────────────────────┐
│ STEP 1: Extract Identity Keys                              │
├─────────────────────────────────────────────────────────────┤
│ • Analyze tables via MCP                                    │
│ • Identify email, phone, user_id columns                    │
│ • Validate data presence                                    │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│ STEP 2: Create Prep Tables                                 │
├─────────────────────────────────────────────────────────────┤
│ • One prep table per key type                               │
│ • Standardize values (lowercase, trim)                      │
│ • Filter invalid/NULL values                                │
│ • Deduplicate within source                                 │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│ STEP 3: Build ID Graph                                     │
├─────────────────────────────────────────────────────────────┤
│ • Connect IDs that share same key value                     │
│ • Create connected components                               │
│ • Iterate until graph stabilizes                            │
│ • Generate canonical ID per component                       │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│ STEP 4: Create Master Records                              │
├─────────────────────────────────────────────────────────────┤
│ • Group by canonical ID                                     │
│ • Select best value per attribute (priority-based)          │
│ • Aggregate arrays of values                                │
│ • Enrich with derived fields                                │
└─────────────────────────────────────────────────────────────┘
```

### Example Data Flow

**Input Tables:**

`customer_profiles`:
```
customer_id | email              | phone
------------|--------------------|-----------
C001        | john@email.com     | 5551234567
C002        | jane@email.com     | 5559876543
```

`order_history`:
```
order_id | user_id | customer_email
---------|---------|----------------
O001     | U001    | john@email.com
O002     | U002    | jane@email.com
O003     | U003    | bob@email.com
```

**Prep Tables:**

`prep_email`:
```
source_table      | key_value       | source_id
------------------|-----------------|----------
customer_profiles | john@email.com  | C001
customer_profiles | jane@email.com  | C002
order_history     | john@email.com  | U001
order_history     | jane@email.com  | U002
order_history     | bob@email.com   | U003
```

**ID Graph Resolution:**
- C001 + U001 → Same email (john@email.com) → Unified ID: UN001
- C002 + U002 → Same email (jane@email.com) → Unified ID: UN002
- U003 → Unique email (bob@email.com) → Unified ID: UN003

**Master Table:**
```
unified_id | best_email      | all_user_ids     | order_count
-----------|-----------------|------------------|------------
UN001      | john@email.com  | [C001, U001]     | 1
UN002      | jane@email.com  | [C002, U002]     | 1
UN003      | bob@email.com   | [U003]           | 1
```

---

## Usage Examples

### Example 1: Complete Setup

```bash
/cdp-unification:unify-setup
```

**Inputs:**
```
Database: ecommerce_stg
Tables: customer_profiles, order_history, email_subscriptions
Keys: email, phone, user_id
Master table: unified_customers
```

**Generated Files:**
- `unification/prep_email.sql`
- `unification/prep_phone.sql`
- `unification/prep_user_id.sql`
- `unify.yml`
- `id_unification.dig`
- `unification/master_customers.sql`

**Result:** Complete unification setup ready to execute

---

### Example 2: Step-by-Step Setup

**Step 1: Extract Keys**
```bash
/cdp-unification:unify-extract-keys
```
Result: JSON with identified columns

**Step 2: Create Preps**
```bash
/cdp-unification:unify-create-prep
```
Result: prep_*.sql files

**Step 3: Create Config**
```bash
/cdp-unification:unify-create-config
```
Result: unify.yml + id_unification.dig

---

## Best Practices

### 1. Key Selection
- Use stable identifiers (email, phone, user_id)
- Avoid frequently changing fields
- Validate key quality before unification
- Consider key priority/reliability

### 2. Data Quality
- Clean data before prep tables
- Standardize formats (lowercase emails, phone format)
- Filter test/invalid data
- Handle NULL values explicitly

### 3. Validation Rules
- Define regex patterns for each key type
- List known invalid values
- Set invalid_texts in unify.yml
- Test with sample data first

### 4. Master Table Design
- Use priority for attribute selection
- Create arrays for multi-valued attributes
- Include metadata (created_at, updated_at)
- Add derived fields as needed

### 5. Performance
- Index prep tables on key_value
- Use incremental unification for updates
- Monitor iteration count
- Optimize merge_iterations setting

---

## Common Issues

### Issue: No Keys Found

**Cause:** MCP couldn't identify PII columns

**Solution:**
- Check column names (should contain "email", "phone", "user_id")
- Manually specify columns if auto-detection fails
- Verify table permissions

### Issue: Too Many Iterations

**Cause:** merge_iterations too high or data quality issues

**Solution:**
- Reduce merge_iterations in unify.yml
- Check for data quality problems
- Review prep table deduplication

### Issue: Missing Canonical IDs

**Cause:** Prep tables empty or invalid keys

**Solution:**
- Verify prep tables have data
- Check validation regex patterns
- Review invalid_texts list

---

## File Structure

```
unification/
├── prep_email.sql
├── prep_phone.sql
├── prep_user_id.sql
├── master_customers.sql
└── validation_report.txt

unify.yml
id_unification.dig
```

---

## Quality Gates

All unification setups must include:

1. **Prep Tables**: One per key type with validation
2. **unify.yml**: Complete configuration with keys, tables, canonical_ids
3. **Workflow**: id_unification.dig with proper dependencies
4. **Master Table**: SQL for unified customer records
5. **Validation**: Regex patterns and invalid_texts defined
6. **Documentation**: Comments explaining logic

---

## Deployment

### Execute Unification

```bash
# Push workflow to TD
td wf push unification_project

# Run workflow
td wf start unification_project id_unification

# Monitor progress
td wf sessions --project unification_project
```

### Verify Results

```sql
-- Check canonical ID coverage
SELECT COUNT(DISTINCT unified_customer_id) AS unified_customers
FROM unified_db.unified_customers;

-- Check key distribution
SELECT
    key_type,
    COUNT(DISTINCT key_value) AS unique_keys,
    COUNT(*) AS total_records
FROM unified_db.prep_email
GROUP BY key_type;
```

---

## Support

For assistance:
- Review generated files in `unification/` directory
- Check MCP extraction results
- Verify prep table SQL
- Test with small dataset first

---

**Version:** 1.3.0
**Last Updated:** 2024-10-13
**Maintained by:** APS CDP Team
