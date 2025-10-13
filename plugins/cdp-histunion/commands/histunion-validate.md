---
name: histunion-validate
description: Validate hist-union workflow and SQL files against production quality gates
---

# Validate Hist-Union Workflows

## ⚠️ CRITICAL: This command validates all hist-union files against production quality gates

I'll help you validate your hist-union workflow files to ensure they meet production standards.

---

## What Gets Validated

### 1. Workflow File Structure
**File**: `hist_union/hist_union_runner.dig`

Checks:
- ✅ Valid YAML syntax
- ✅ Required sections present (timezone, _export, tasks)
- ✅ inc_log table creation task exists
- ✅ hist_union_tasks section present
- ✅ `_parallel: true` configured for concurrent execution
- ✅ No schedule block (schedules should be external)
- ✅ Correct lkup_db variable usage
- ✅ All SQL files referenced exist

### 2. SQL File Structure
**Files**: `hist_union/queries/*.sql`

For each SQL file, checks:
- ✅ Valid SQL syntax (Presto/Trino compatible)
- ✅ CREATE TABLE statement present
- ✅ INSERT INTO with UNION ALL structure
- ✅ Watermark filtering using inc_log (for incremental tables)
- ✅ Watermark updates for both hist and inc tables
- ✅ Correct project_name = 'hist_union' in watermark updates
- ✅ No backticks (use double quotes for reserved keywords)
- ✅ Consistent table naming (inc, hist, histunion)

### 3. Schema Validation
**Requires MCP access to Treasure Data**

For each table pair, checks:
- ✅ Inc table exists and is accessible
- ✅ Hist table exists and is accessible
- ✅ CREATE TABLE columns match inc table schema
- ✅ Column order matches inc table schema
- ✅ NULL handling for columns missing in hist table
- ✅ All inc table columns present in SQL
- ✅ UNION ALL column counts match

### 4. Template Compliance
Checks against template requirements:
- ✅ Full load tables use correct template (DROP + no WHERE)
- ✅ Incremental tables use correct template (CREATE IF NOT EXISTS + WHERE)
- ✅ Watermark updates present for both tables
- ✅ COALESCE used for watermark defaults
- ✅ Correct table name variables used

---

## Validation Modes

### Mode 1: Syntax Validation (Fast)
**No MCP required** - Validates file structure and SQL syntax only
```bash
Use when: Quick syntax check without database access
Checks: File structure, YAML syntax, SQL syntax, basic patterns
Duration: ~10 seconds
```

### Mode 2: Schema Validation (Complete)
**Requires MCP** - Validates against actual table schemas
```bash
Use when: Pre-deployment validation, full compliance check
Checks: Everything in Mode 1 + schema matching, column validation
Duration: ~30-60 seconds (depends on table count)
```

---

## What I'll Do

### Step 1: Scan Files
```
Scanning hist_union directory...
✅ Found workflow file: hist_union_runner.dig
✅ Found N SQL files in queries/
```

### Step 2: Validate Workflow File
```
Validating hist_union_runner.dig...
✅ YAML syntax valid
✅ timezone set to UTC
✅ _export section present with td.database and lkup_db
✅ +create_inc_log_table task present
✅ +hist_union_tasks section present
✅ _parallel: true configured
✅ No schedule block found
✅ All referenced SQL files exist
```

### Step 3: Validate Each SQL File
```
Validating hist_union/queries/klaviyo_events.sql...
✅ SQL syntax valid (Presto/Trino compatible)
✅ CREATE TABLE statement found
✅ Table name: mck_src.klaviyo_events_histunion
✅ INSERT INTO with UNION ALL structure found
✅ Watermark filtering present for hist table
✅ Watermark filtering present for inc table
✅ Watermark update for hist table found
✅ Watermark update for inc table found
✅ project_name = 'hist_union' verified
✅ No backticks found (using double quotes)
```

### Step 4: Schema Validation (Mode 2 Only)
```
Validating schemas via MCP tool...

Table: klaviyo_events
✅ Inc table exists: mck_src.klaviyo_events
✅ Hist table exists: mck_src.klaviyo_events_hist
✅ Retrieved inc schema: 45 columns
✅ Retrieved hist schema: 44 columns
✅ Schema difference: inc has 'incremental_date', hist does not
✅ CREATE TABLE matches inc schema (45 columns)
✅ Column order matches inc schema
✅ NULL handling present for 'incremental_date' in hist SELECT
✅ All 45 inc columns present in SQL
✅ UNION ALL column counts match (45 = 45)
```

### Step 5: Template Compliance Check
```
Checking template compliance...

Table: klaviyo_lists
⚠️  Full load table detected
✅ Uses Case 3 template (DROP TABLE + no WHERE clause)
✅ Watermarks still updated

Table: klaviyo_events
✅ Incremental table
✅ Uses Case 2 template (inc has extra columns)
✅ CREATE TABLE IF NOT EXISTS used
✅ WHERE clauses present for watermark filtering
✅ COALESCE with default value 0
```

### Step 6: Generate Validation Report
```
Generating validation report...
✅ Report created with all findings
✅ Errors highlighted (if any)
✅ Warnings noted (if any)
✅ Recommendations provided (if any)
```

---

## Validation Report Format

### Summary Section
```
═══════════════════════════════════════════════════════════
HIST-UNION VALIDATION REPORT
═══════════════════════════════════════════════════════════

Validation Mode: [Syntax Only / Full Schema Validation]
Timestamp: 2024-10-13 14:30:00 UTC
Workflow File: hist_union/hist_union_runner.dig
SQL Files: 5

Overall Status: ✅ PASSED / ❌ FAILED / ⚠️  WARNINGS
```

### Detailed Results
```
───────────────────────────────────────────────────────────
WORKFLOW FILE: hist_union_runner.dig
───────────────────────────────────────────────────────────
✅ YAML Syntax: Valid
✅ Structure: Complete (all required sections present)
✅ Parallel Execution: Configured (_parallel: true)
✅ inc_log Task: Present
✅ Schedule: None (correct)
✅ SQL References: All 5 files exist

───────────────────────────────────────────────────────────
SQL FILE: queries/klaviyo_events.sql
───────────────────────────────────────────────────────────
✅ SQL Syntax: Valid (Presto/Trino)
✅ Template: Case 2 (Inc has extra columns)
✅ Table: mck_src.klaviyo_events_histunion
✅ CREATE TABLE: Present
✅ UNION ALL: Correct structure
✅ Watermarks: Both hist and inc updates present
✅ NULL Handling: Correct for 'incremental_date'
✅ Schema Match: All 45 columns present in correct order

───────────────────────────────────────────────────────────
SQL FILE: queries/klaviyo_lists.sql
───────────────────────────────────────────────────────────
✅ SQL Syntax: Valid (Presto/Trino)
✅ Template: Case 3 (Full load)
⚠️  Table Type: FULL LOAD table
✅ DROP TABLE: Present
✅ CREATE TABLE: Correct (no IF NOT EXISTS)
✅ WHERE Clauses: Absent (correct for full load)
✅ UNION ALL: Correct structure
✅ Watermarks: Both hist and inc updates present
✅ Schema Match: All 52 columns present in correct order

... (for all SQL files)
```

### Issues Section (if any)
```
───────────────────────────────────────────────────────────
ISSUES FOUND
───────────────────────────────────────────────────────────

❌ ERROR: queries/shopify_products.sql
   - Line 15: Column 'incremental_date' missing in CREATE TABLE
   - Expected: 'incremental_date varchar' based on inc table schema
   - Fix: Add 'incremental_date varchar' to CREATE TABLE statement

❌ ERROR: queries/users.sql
   - Line 45: Using backticks around column "index"
   - Fix: Replace `index` with "index" (Presto/Trino requires double quotes)

⚠️  WARNING: hist_union_runner.dig
   - Line 25: Task +shopify_variants_histunion references non-existent SQL file
   - Expected: queries/shopify_variants.sql
   - Fix: Create missing SQL file or remove task

⚠️  WARNING: queries/onetrust_profiles.sql
   - Missing watermark update for hist table
   - Should have: INSERT INTO inc_log for onetrust_profiles_hist
   - Fix: Add watermark update after UNION ALL insert
```

### Recommendations Section
```
───────────────────────────────────────────────────────────
RECOMMENDATIONS
───────────────────────────────────────────────────────────

💡 Consider adding these improvements:
   1. Add comments to SQL files explaining schema differences
   2. Document which tables are full load vs incremental
   3. Add error handling tasks in workflow
   4. Consider adding validation queries after inserts

💡 Performance optimizations:
   1. Review parallel task limit based on TD account
   2. Consider partitioning very large tables
   3. Review watermark index on inc_log table
```

---

## Error Categories

### Critical Errors (Must Fix)
- ❌ Invalid YAML syntax in workflow
- ❌ Invalid SQL syntax
- ❌ Missing required sections (CREATE, INSERT, watermarks)
- ❌ Column count mismatch in UNION ALL
- ❌ Schema mismatch with inc table
- ❌ Referenced SQL files don't exist
- ❌ Inc or hist table doesn't exist in TD

### Warnings (Should Fix)
- ⚠️  Using backticks instead of double quotes
- ⚠️  Missing NULL handling for extra columns
- ⚠️  Wrong template for full load tables
- ⚠️  Watermark updates incomplete
- ⚠️  Column order doesn't match schema

### Info (Nice to Have)
- ℹ️  Could add more comments
- ℹ️  Could optimize query structure
- ℹ️  Could add data validation queries

---

## Usage Examples

### Example 1: Quick Syntax Check
```
User: "Validate my hist-union files"

I will:
1. Scan hist_union directory
2. Validate workflow YAML syntax
3. Validate all SQL file syntax
4. Check file references
5. Generate report with findings
```

### Example 2: Full Validation with Schema Check
```
User: "Validate hist-union files with full schema check"

I will:
1. Scan hist_union directory
2. Validate workflow and SQL syntax
3. Use MCP tool to get all table schemas
4. Compare CREATE TABLE with actual schemas
5. Verify column order and NULL handling
6. Check template compliance
7. Generate comprehensive report
```

### Example 3: Validate Specific File
```
User: "Validate just the klaviyo_events.sql file"

I will:
1. Read queries/klaviyo_events.sql
2. Validate SQL syntax
3. Check template structure
4. Optionally get schema via MCP
5. Generate focused report for this file
```

---

## Next Steps After Validation

### If Validation Passes
```bash
✅ All checks passed!

Next steps:
1. Deploy to Treasure Data: td wf push hist_union
2. Run workflow: td wf run hist_union_runner.dig
3. Monitor execution: td wf logs hist_union_runner.dig
4. Verify results in target tables
```

### If Validation Fails
```bash
❌ Validation found N errors and M warnings

Next steps:
1. Review validation report for details
2. Fix all critical errors (❌)
3. Address warnings (⚠️ ) if possible
4. Re-run validation
5. Deploy only after all errors are resolved
```

---

## Production-Ready Checklist

Before deploying, ensure:
- [ ] Workflow file YAML syntax is valid
- [ ] All SQL files have valid Presto/Trino syntax
- [ ] All referenced SQL files exist
- [ ] inc_log table creation task present
- [ ] Parallel execution configured
- [ ] No schedule blocks in workflow
- [ ] All CREATE TABLE statements match inc schemas
- [ ] Column order matches inc table schemas
- [ ] NULL handling present for schema differences
- [ ] Watermark updates present for all tables
- [ ] Full load tables use correct template
- [ ] No backticks in SQL (use double quotes)
- [ ] All table references are correct

---

**Ready to validate? Specify validation mode (syntax-only or full-schema) and I'll run comprehensive validation against all production quality gates.**
