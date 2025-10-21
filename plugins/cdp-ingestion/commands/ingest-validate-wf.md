---
name: ingest-validate-wf
description: Validate Digdag workflow and configuration files against production quality gates
---

# Validate Ingestion Workflow

## ⚠️ CRITICAL: This validates against strict production quality gates

I'll validate your ingestion workflow for compliance with production standards and best practices.

---

## What I'll Validate

### Quality Gates (ALL MUST PASS)

#### 1. Template Compliance
- ✅ Code matches documented templates 100%
- ✅ No unauthorized deviations from patterns
- ✅ All template sections present
- ✅ Exact formatting and structure

#### 2. Logging Requirements
- ✅ Start logging before data processing
- ✅ Success logging after td_load
- ✅ Error logging in `_error` blocks
- ✅ Minimum 3 logging blocks per data source
- ✅ Correct SQL template usage

#### 3. Error Handling
- ✅ `_error:` blocks present in all workflows
- ✅ Error logging with SQL present
- ✅ Proper error message capture
- ✅ Job ID and URL captured in errors

#### 4. Timestamp Format
- ✅ Correct format for connector type:
  - Google BigQuery: SQL Server format (`CONVERT(varchar, ..., 121)`)
  - Klaviyo: `.000000` (6 decimals, NO Z)
  - OneTrust: `.000Z` (3 decimals, WITH Z)
  - Shopify v2: ISO 8601
- ✅ Matches `docs/patterns/timestamp-formats.md`

#### 5. Incremental Field Handling
- ✅ Correct field names (table vs. API)
- ✅ Dual field handling where needed (Klaviyo campaigns)
- ✅ Proper COALESCE fallback logic
- ✅ Matches `docs/patterns/incremental-patterns.md`

#### 6. Workflow Structure
- ✅ Must match `docs/patterns/workflow-patterns.md`
- ✅ Proper timezone declaration (`timezone: UTC`)
- ✅ Correct `_export` includes
- ✅ Proper task naming conventions
- ✅ Correct file organization
- ✅ Parallel processing limits appropriate for source

#### 7. Configuration Files
- ✅ YAML syntax validity
- ✅ Secret references (`${secret:name}`) used correctly
- ✅ No hardcoded credentials
- ✅ Required parameters present
- ✅ Database references correct
- ✅ Mode set appropriately (`append`, `replace`)

#### 8. File Organization
- ✅ `.dig` files in `ingestion/` directory
- ✅ YAML configs in `ingestion/config/` subdirectory
- ✅ SQL files in `ingestion/sql/` subdirectory
- ✅ Proper file naming conventions

#### 9. Security
- ✅ No hardcoded credentials in any file
- ✅ Proper `${secret:name}` syntax usage
- ✅ `credentials_ingestion.json` NOT in version control
- ✅ `.gitignore` includes credentials file

---

## Validation Options

### Option 1: Validate Specific Workflow
Provide:
- **Workflow file path**: e.g., `ingestion/klaviyo_ingest_inc.dig`
- **Related config files**: (or I'll find them automatically)

I will:
1. Read the workflow file
2. Find all related config files
3. Check against ALL quality gates
4. Report detailed findings with line numbers

### Option 2: Validate Entire Source
Provide:
- **Source name**: e.g., `klaviyo`, `shopify_v2`, `google_bigquery`

I will:
1. Find all workflows for the source
2. Find all config files for the source
3. Validate against source-specific documentation
4. Check all quality gates
5. Report comprehensive findings

### Option 3: Validate All
Say: **"validate all"**

I will:
1. Find all workflows in `ingestion/`
2. Find all configs in `ingestion/config/`
3. Validate each against its source documentation
4. Check all quality gates
5. Report full project compliance status

---

## Validation Process

### Step 1: Read Documentation
I will read relevant documentation to verify compliance:
- Source-specific docs: `docs/sources/{source-name}.md`
- Pattern docs: `docs/patterns/*.md`

### Step 2: Load Files
I will read all specified workflow and config files

### Step 3: Check Quality Gates
I will verify each file against ALL quality gates listed above

### Step 4: Report Findings

#### Pass Report (if all gates pass)
```
✅ VALIDATION PASSED

Workflow: ingestion/{source}_ingest_inc.dig
Source: {source}

Quality Gates: 9/9 PASSED
✅ Template Compliance
✅ Logging Requirements
✅ Error Handling
✅ Timestamp Format
✅ Incremental Fields
✅ Workflow Structure
✅ Configuration Files
✅ File Organization
✅ Security

No issues found. Workflow is production-ready.
```

#### Fail Report (if any gate fails)
```
❌ VALIDATION FAILED

Workflow: ingestion/{source}_ingest_inc.dig
Source: {source}

Quality Gates: 6/9 PASSED

✅ Template Compliance
✅ Logging Requirements
❌ Error Handling - FAILED
  - Missing _error block in main workflow
  - Error logging SQL not found

✅ Timestamp Format
❌ Incremental Fields - FAILED
  - Using wrong field name: 'updated_at' should be 'updated' for API
  - Line 45: incremental_field parameter incorrect

✅ Workflow Structure
✅ Configuration Files
✅ File Organization
❌ Security - FAILED
  - Hardcoded API key found in config/klaviyo_profiles_load.yml:12
  - Should use ${secret:klaviyo_api_key}

RECOMMENDATIONS:
1. Add _error block to main workflow (see docs/patterns/workflow-patterns.md)
2. Fix incremental field name (see docs/sources/klaviyo.md)
3. Replace hardcoded credential with secret reference

Re-validate after fixing issues.
```

---

## Common Issues Detected

### Template Violations
- Simplified or "optimized" templates
- Removed "redundant" sections
- Modified variable names
- Changed structure

### Logging Violations
- Missing start/success/error logging
- Incorrect SQL template usage
- Missing job ID or URL capture

### Timestamp Format Errors
- Wrong decimal count
- Missing or incorrect timezone marker
- Using default instead of connector-specific format

### Incremental Field Errors
- Using table field name in API parameter
- Using API field name in SQL queries
- Missing COALESCE fallback

### Security Issues
- Hardcoded credentials
- Incorrect secret syntax
- Credentials file in version control

---

## Next Steps After Validation

### If Validation Passes
✅ Workflow is production-ready
- Deploy with confidence
- Monitor ingestion_log for ongoing health

### If Validation Fails
❌ Fix reported issues:
1. Re-read relevant documentation
2. Apply exact templates
3. Fix specific line numbers mentioned
4. Re-validate until all gates pass

**DO NOT deploy failing workflows to production**

---

## Production Quality Assurance

This validation ensures:
- ✅ Code works the first time
- ✅ Consistent patterns across sources
- ✅ Complete error handling and logging
- ✅ Maintainable and documented code
- ✅ No security vulnerabilities
- ✅ Compliance with team standards

---

**What would you like to validate?**

Options:
1. Validate specific workflow: Provide workflow file path
2. Validate entire source: Provide source name
3. Validate all: Say "validate all"
