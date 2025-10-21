---
name: transform-validation
description: Validate staging transformation files against CLAUDE.md compliance and quality gates
---

# Validate Staging Transformation

## ⚠️ CRITICAL: This validates against strict production quality gates

I'll validate your staging transformation files for compliance with CLAUDE.md specifications and production standards.

---

## What I'll Validate

### Quality Gates (ALL MUST PASS)

#### 1. File Structure Compliance
- ✅ Correct directory structure (staging/ or staging_hive/)
- ✅ Proper file naming conventions
- ✅ All required files present
- ✅ No missing dependencies

#### 2. SQL File Validation
- ✅ **Incremental SQL** (`staging/queries/{source_db}_{table}.sql`)
  - Correct FROM clause with `{source_database}.{source_table}`
  - Correct WHERE clause with incremental logic
  - Proper database references
- ✅ **Initial Load SQL** (`staging/init_queries/{source_db}_{table}_init.sql`)
  - Full table scan (no WHERE clause)
  - Same transformations as incremental
- ✅ **Upsert SQL** (`staging/queries/{source_db}_{table}_upsert.sql`)
  - Only present if deduplication exists
  - Correct DELETE and INSERT logic
  - Work table pattern used

#### 3. Data Transformation Standards
- ✅ **Column Processing**:
  - All columns transformed (except 'time')
  - Column limit compliance (max 200)
  - Proper type casting
- ✅ **Date Columns** (CRITICAL):
  - ALL date columns have 4 outputs (original, _std, _unixtime, _date)
  - `time` column NOT transformed
  - Correct FORMAT_DATETIME patterns (Presto) or date_format (Hive)
- ✅ **JSON Columns** (CRITICAL):
  - ALL JSON columns detected and processed
  - Top-level key extraction completed
  - Nested objects handled (up to 2 levels)
  - Array handling with TRY_CAST
  - All extractions wrapped with NULLIF(UPPER(...), '')
- ✅ **Email Columns**:
  - Original + _std + _hash + _valid outputs
  - Correct validation regex
  - SHA256 hashing applied
- ✅ **Phone Columns**:
  - Exact pattern compliance
  - phone_std, phone_hash, phone_valid outputs
  - No deviations from templates

#### 4. Data Processing Order
- ✅ **Clean → Join → Dedupe sequence followed**
- ✅ Joins use cleaned columns (not raw)
- ✅ Deduplication uses cleaned columns (not raw)
- ✅ No raw column names in PARTITION BY
- ✅ Proper CTE structure

#### 5. Configuration File Validation
- ✅ **src_params.yml structure**:
  - Valid YAML syntax
  - All required fields present
  - Correct dependency group structure
  - Proper table configuration
- ✅ **Table Configuration**:
  - `name`, `source_db`, `staging_table` present
  - `has_dedup` boolean correct
  - `partition_columns` matches dedup logic
  - `mode` set correctly (inc/full)

#### 6. DIG File Validation
- ✅ **Loop-based architecture** used
- ✅ No repetitive table blocks
- ✅ Correct template structure
- ✅ Proper error handling
- ✅ Inc_log table creation
- ✅ Conditional processing logic

#### 7. Treasure Data Compatibility
- ✅ **Forbidden Types**:
  - No TIMESTAMP types (must be VARCHAR/BIGINT)
  - No BOOLEAN types (must be VARCHAR)
  - No DOUBLE for timestamps
- ✅ **Function Compatibility**:
  - Presto: FORMAT_DATETIME, TD_TIME_PARSE
  - Hive: date_format, unix_timestamp, from_unixtime
- ✅ **Type Casting**:
  - TRY_CAST used for safety
  - All timestamps cast to VARCHAR or BIGINT

#### 8. Incremental Processing
- ✅ **State Management**:
  - Correct inc_log table usage
  - COALESCE(MAX(inc_value), 0) pattern
  - Proper WHERE clause filtering
- ✅ **Database References**:
  - Source table: `{source_database}.{source_table}`
  - Inc_log: `${lkup_db}.inc_log`
  - Correct project_name filter

#### 9. Engine-Specific Validation

**For Presto/Trino:**
- ✅ Files in `staging/` directory
- ✅ Three SQL files (init, incremental, upsert if dedup)
- ✅ Presto-compatible functions used
- ✅ JSON: json_extract_scalar with NULLIF(UPPER(...), '')

**For Hive:**
- ✅ Files in `staging_hive/` directory
- ✅ Single SQL file with INSERT OVERWRITE
- ✅ Template files present (get_max_time.sql, get_stg_rows_for_delete.sql)
- ✅ Hive-compatible functions used
- ✅ JSON: REGEXP_EXTRACT with NULLIF(UPPER(...), '')
- ✅ Timestamp keyword escaped with backticks

---

## Validation Options

### Option 1: Validate Specific Table
Provide:
- **Table Name**: e.g., `client_src.customer_profiles`
- **Engine**: `presto` or `hive`

I will:
1. Find all related files for the table
2. Check against ALL quality gates
3. Report detailed findings with line numbers
4. Provide remediation guidance

### Option 2: Validate All Tables
Say: **"validate all"** or **"validate all Presto"** or **"validate all Hive"**

I will:
1. Find all transformation files
2. Validate each table against quality gates
3. Report comprehensive project status
4. Identify any inconsistencies

### Option 3: Validate Configuration Only
Say: **"validate config"**

I will:
1. Check `staging/config/src_params.yml` (Presto)
2. Check `staging_hive/config/src_params.yml` (Hive)
3. Validate YAML syntax
4. Verify table configurations
5. Check dependency groups

---

## Validation Process

### Step 1: File Discovery
I will locate all relevant files:
- SQL files (init, queries, upsert)
- Configuration files (src_params.yml)
- Workflow files (staging_transformation.dig or staging_hive.dig)

### Step 2: Load and Parse
I will read all files:
- Parse SQL syntax
- Parse YAML structure
- Extract transformation logic
- Identify patterns

### Step 3: Quality Gate Checks
I will verify each gate systematically:
- Execute all validation rules
- Identify violations
- Collect evidence (line numbers, code samples)
- Categorize severity

### Step 4: Generate Report

#### Pass Report (if all gates pass)
```
✅ VALIDATION PASSED

Table: client_src.customer_profiles
Engine: Presto
Files validated: 3

Quality Gates: 9/9 PASSED
✅ File Structure Compliance
✅ SQL File Validation
✅ Data Transformation Standards
✅ Data Processing Order
✅ Configuration File Validation
✅ DIG File Validation
✅ Treasure Data Compatibility
✅ Incremental Processing
✅ Engine-Specific Validation

Transformation Details:
- Columns: 45 (within 200 limit)
- Date columns: 3 (12 outputs - all have 4 outputs ✅)
- JSON columns: 2 (all processed ✅)
- Email columns: 1 (validated with hashing ✅)
- Phone columns: 1 (exact pattern compliance ✅)
- Deduplication: Yes (customer_id)
- Joins: None

No issues found. Transformation is production-ready.
```

#### Fail Report (if any gate fails)
```
❌ VALIDATION FAILED

Table: client_src.customer_profiles
Engine: Presto
Files validated: 3

Quality Gates: 6/9 PASSED

✅ File Structure Compliance
✅ SQL File Validation
❌ Data Transformation Standards - FAILED
  - Date column 'created_at' missing outputs
    Line 45: Only has _std output, missing _unixtime and _date
  - JSON column 'attributes' not processed
    Line 67: Raw column passed through without extraction

✅ Data Processing Order
✅ Configuration File Validation
❌ DIG File Validation - FAILED
  - Using old repetitive block structure
    File: staging/staging_transformation.dig
    Issue: Should use loop-based architecture

✅ Treasure Data Compatibility
✅ Incremental Processing
❌ Engine-Specific Validation - FAILED
  - JSON extraction missing NULLIF(UPPER(...), '') wrapper
    Line 72: json_extract_scalar used without wrapper
    Should be: NULLIF(UPPER(json_extract_scalar(...)), '')

CRITICAL ISSUES (Must Fix):
1. Add missing date outputs for 'created_at' column
2. Process 'attributes' JSON column with key extraction
3. Migrate DIG file to loop-based architecture
4. Wrap all JSON extractions with NULLIF(UPPER(...), '')

RECOMMENDATIONS:
- Re-read CLAUDE.md date processing requirements
- Check JSON column detection logic
- Use DIG template from documentation
- Review JSON extraction patterns

Re-validate after fixing issues.
```

---

## Common Issues Detected

### Data Transformation Violations
- Missing date column outputs (not all 4)
- JSON columns not processed
- Raw columns used in deduplication
- Phone/email patterns deviated from templates

### Configuration Violations
- Invalid YAML syntax
- Missing required fields
- Incorrect dependency group structure
- Table name mismatches

### DIG File Violations
- Old repetitive block structure
- Missing error handling
- Incorrect template variables
- No inc_log creation

### Engine-Specific Violations
- **Presto**: Wrong date functions, missing TRY_CAST
- **Hive**: REGEXP_EXTRACT patterns wrong, timestamp not escaped

### Incremental Processing Violations
- Wrong WHERE clause pattern
- Incorrect database references
- Missing COALESCE in inc_log lookup

---

## Remediation Guidance

### For Each Failed Gate:
1. **Reference CLAUDE.md** - Re-read relevant section
2. **Check Examples** - Review sample code in documentation
3. **Fix Violations** - Apply exact patterns from templates
4. **Re-validate** - Run validation again until passes

### Quick Fixes:

**Date Columns Missing Outputs:**
```sql
-- Add all 4 outputs for each date column
column AS column,
FORMAT_DATETIME(...) AS column_std,
TD_TIME_PARSE(...) AS column_unixtime,
SUBSTR(...) AS column_date
```

**JSON Columns Not Processed:**
```sql
-- Sample data, extract keys, add extractions
NULLIF(UPPER(json_extract_scalar(json_col, '$.key')), '') AS json_col_key
```

**DIG File Old Structure:**
- Delete old file
- Use loop-based template from documentation
- Update src_params.yml instead

---

## Next Steps After Validation

### If Validation Passes
✅ Transformation is production-ready:
- Deploy with confidence
- Test workflow execution
- Monitor inc_log for health

### If Validation Fails
❌ Fix reported issues:
1. Address critical issues first
2. Apply exact patterns from CLAUDE.md
3. Re-validate until all gates pass
4. **DO NOT deploy failing transformations**

---

## Production Quality Assurance

This validation ensures:
- ✅ Transformations work correctly
- ✅ Data quality maintained
- ✅ No data loss or corruption
- ✅ Consistent patterns across tables
- ✅ Maintainable code
- ✅ Compliance with standards

---

**What would you like to validate?**

Options:
1. **Validate specific table**: Provide table name and engine
2. **Validate all tables**: Say "validate all"
3. **Validate configuration**: Say "validate config"
