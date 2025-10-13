---
name: unify-extract-keys
description: Extract and validate user identifier columns from tables using live Treasure Data analysis
---

# Extract and Validate User Identifiers

## Overview

I'll analyze your Treasure Data tables to extract and validate user identifier columns using the **unif-keys-extractor** specialized agent.

This command performs **ZERO-TOLERANCE** identifier extraction:
- ❌ **NO GUESSING** - Only uses real Treasure Data MCP tools
- ❌ **NO ASSUMPTIONS** - Every table is analyzed with live data
- ✅ **STRICT VALIDATION** - Only includes tables with actual user identifiers
- ✅ **COMPREHENSIVE ANALYSIS** - 3 SQL experts review and priority recommendations

---

## What You Need to Provide

### Table List
Provide the tables you want to analyze for ID unification:
- **Format**: `database.table_name`
- **Example**: `analytics.user_events`, `crm.customers`, `web.pageviews`

---

## What I'll Do

### Step 1: Schema Extraction (MANDATORY)
For each table, I'll:
- Call `mcp__mcc_treasuredata__describe_table(table, database)`
- Extract EXACT column names and data types
- Identify tables that are inaccessible

### Step 2: User Identifier Detection (STRICT MATCHING)
I'll scan for valid user identifier columns:

**✅ VALID USER IDENTIFIERS:**
- **Email columns**: email, email_std, email_address, user_email, customer_email
- **Phone columns**: phone, phone_std, phone_number, mobile_phone, customer_phone
- **User ID columns**: user_id, customer_id, account_id, member_id, uid, user_uuid
- **Identity columns**: profile_id, identity_id, cognito_identity_userid
- **Cookie/Device IDs**: td_client_id, td_global_id, td_ssc_id, cookie_id, device_id

**❌ NOT USER IDENTIFIERS (EXCLUDED):**
- System columns: id, created_at, updated_at, load_timestamp
- Campaign columns: campaign_id, message_id
- Product columns: product_id, sku, variant_id
- Complex types: array, map, json columns

### Step 3: Exclusion Validation (CRITICAL)
For tables WITHOUT user identifiers, I'll:
- Document the exclusion reason
- List available columns for transparency
- Explain why the table doesn't qualify

### Step 4: Min/Max Data Analysis (INCLUDED TABLES ONLY)
For tables WITH user identifiers, I'll:
- Query actual data: `SELECT MIN(column), MAX(column) FROM table`
- Validate data patterns and formats
- Assess data quality

### Step 5: 3 SQL Experts Analysis
I'll provide structured analysis from three perspectives:
1. **Data Pattern Analyst**: Reviews actual min/max values and data quality
2. **Cross-Table Relationship Analyst**: Maps identifier relationships across tables
3. **Priority Assessment Specialist**: Ranks identifiers by stability and coverage

### Step 6: Priority Recommendations
I'll provide:
- Recommended priority ordering (TD standard)
- Reasoning for each recommendation
- Compatibility assessment across tables

---

## Expected Output

### Key Extraction Results Table
```
| database_name | table_name | column_name | data_type | identifier_type | min_value | max_value |
|---------------|------------|-------------|-----------|-----------------|-----------|-----------|
| analytics     | user_events| user_email  | varchar   | email           | a@test.com| z@test.com|
| analytics     | user_events| td_client_id| varchar   | cookie_id       | 00000000-.| ffffffff-.|
| crm           | customers  | email       | varchar   | email           | admin@... | user@...  |
```

### Exclusion Documentation
```
## Tables EXCLUDED from ID Unification:

- **analytics.product_catalog**: No user identifier columns found
  - Available columns: [product_id, sku, product_name, category, price]
  - Exclusion reason: Contains only product metadata - no PII
  - Classification: Non-PII table
```

### Validation Summary
```
## Analysis Summary:
- **Tables Analyzed**: 5
- **Tables INCLUDED**: 3 (contain user identifiers)
- **Tables EXCLUDED**: 2 (no user identifiers)
- **User Identifier Columns Found**: 8
```

### 3 SQL Experts Analysis
```
**Expert 1 - Data Pattern Analyst:**
- Email columns show valid format patterns across 2 tables
- td_client_id shows UUID format with good coverage
- Data quality: High (95%+ non-null for email)

**Expert 2 - Cross-Table Relationship Analyst:**
- Email appears in analytics.user_events and crm.customers (primary link)
- td_client_id unique to analytics.user_events (secondary ID)
- Recommendation: Email as primary key for unification

**Expert 3 - Priority Assessment Specialist:**
- Priority 1: email (stable, cross-table presence, good coverage)
- Priority 2: td_client_id (system-generated, analytics-specific)
- Recommended merge_by_keys: [email, td_client_id]
```

### Priority Recommendations (TD Standard)
```
Recommended Priority Order (TD Standard):
1. email - Stable identifier across multiple tables with high coverage
2. td_client_id - System-generated ID for analytics tracking
3. phone - Secondary contact identifier (if available)

EXCLUDED Identifiers (Not User-Related):
- product_id - Product reference, not user identifier
- campaign_id - Campaign metadata, not user-specific
```

---

## Validation Gates

I'll pass through these mandatory validation gates:
- ✅ **GATE 1**: Schema extracted for all accessible tables
- ✅ **GATE 2**: Tables classified into INCLUSION/EXCLUSION lists
- ✅ **GATE 3**: All exclusions justified and documented
- ✅ **GATE 4**: Real data analysis completed for included columns
- ✅ **GATE 5**: 3 SQL experts analysis completed
- ✅ **GATE 6**: Priority recommendations provided

---

## Next Steps

After key extraction, you can:
1. **Proceed with full setup**: Use `/cdp-unification:unify-setup` to continue with complete configuration
2. **Create prep tables**: Use `/cdp-unification:unify-create-prep` with the extracted keys
3. **Review and adjust**: Discuss the results and make adjustments to table selection

---

## Communication Pattern

I'll use **TD Copilot standard format**:

**Question**: Are these extracted user identifiers sufficient for your ID unification requirements?

**Suggestion**: I recommend using **email** as your primary unification key since it appears across multiple tables with good data quality.

**Check Point**: The analysis shows X tables with user identifiers and Y tables excluded. This provides comprehensive coverage for customer identity resolution.

---

## Getting Started

**Ready to extract user identifiers?** Please provide your table list:

**Example:**
```
Please analyze these tables for ID unification:
- analytics.user_events
- crm.customers
- web.pageviews
- marketing.campaigns
```

I'll call the **unif-keys-extractor** agent to perform comprehensive analysis with ZERO-TOLERANCE validation.

---

**Let's begin the analysis!**
