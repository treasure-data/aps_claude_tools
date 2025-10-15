---
name: hybrid-unif-config-validate
description: Validate YAML configuration for hybrid ID unification before SQL generation
---

# Validate Hybrid ID Unification YAML

## Overview

Validate your `unify.yml` configuration file to ensure it's properly structured and ready for SQL generation. This command checks syntax, structure, validation rules, and provides recommendations for optimization.

---

## What You Need

### Required Input
1. **YAML Configuration File**: Path to your `unify.yml`

---

## What I'll Do

### Step 1: File Validation
- Verify file exists and is readable
- Check YAML syntax (proper indentation, quotes, etc.)
- Ensure all required sections are present

### Step 2: Structure Validation
Check presence and structure of:
- **name**: Unification project name
- **keys**: Key definitions with validation rules
- **tables**: Source tables with key column mappings
- **canonical_ids**: Canonical ID configuration
- **master_tables**: Master table definitions (optional)

### Step 3: Content Validation
Validate individual sections:

**Keys Section**:
- ✓ Each key has a unique name
- ✓ `valid_regexp` is a valid regex pattern (if provided)
- ✓ `invalid_texts` is an array (if provided)
- ⚠ Recommend validation rules if missing

**Tables Section**:
- ✓ Each table has a name
- ✓ Each table has at least one key_column
- ✓ All referenced keys exist in keys section
- ✓ Column names are valid identifiers
- ⚠ Check for duplicate table definitions

**Canonical IDs Section**:
- ✓ Has a name (will be canonical ID column name)
- ✓ `merge_by_keys` references existing keys
- ✓ `merge_iterations` is a positive integer (if provided)
- ⚠ Suggest optimal iteration count if not specified

**Master Tables Section** (if present):
- ✓ Each master table has a name and canonical_id
- ✓ Referenced canonical_id exists
- ✓ Attributes have proper structure
- ✓ Source tables in attributes exist
- ✓ Priority values are valid
- ⚠ Check for attribute conflicts

### Step 4: Cross-Reference Validation
- ✓ All merge_by_keys exist in keys section
- ✓ All key_columns reference defined keys
- ✓ All master table source tables exist in tables section
- ✓ Canonical ID names don't conflict with existing columns

### Step 5: Best Practices Check
Provide recommendations for:
- Key validation rules
- Iteration count optimization
- Master table attribute priorities
- Performance considerations

### Step 6: Validation Report
Generate comprehensive report with:
- ✅ Passed checks
- ⚠ Warnings (non-critical issues)
- ❌ Errors (must fix before generation)
- 💡 Recommendations for improvement

---

## Command Usage

### Basic Usage
```
/cdp-hybrid-idu:hybrid-unif-config-validate

I'll prompt you for:
- YAML file path
```

### Direct Usage
```
YAML file: /path/to/unify.yml
```

---

## Example Validation

### Input YAML
```yaml
name: customer_unification

keys:
  - name: email
    valid_regexp: ".*@.*"
    invalid_texts: ['', 'N/A', 'null']
  - name: customer_id
    invalid_texts: ['', 'N/A']

tables:
  - table: customer_profiles
    key_columns:
      - {column: email_std, key: email}
      - {column: customer_id, key: customer_id}
  - table: orders
    key_columns:
      - {column: email_address, key: email}

canonical_ids:
  - name: unified_id
    merge_by_keys: [email, customer_id]
    merge_iterations: 15

master_tables:
  - name: customer_master
    canonical_id: unified_id
    attributes:
      - name: best_email
        source_columns:
          - {table: customer_profiles, column: email_std, priority: 1}
          - {table: orders, column: email_address, priority: 2}
```

### Validation Report
```
✅ YAML VALIDATION SUCCESSFUL

File Structure:
  ✅ Valid YAML syntax
  ✅ All required sections present
  ✅ Proper indentation and formatting

Keys Section (2 keys):
  ✅ email: Valid regex pattern, invalid_texts defined
  ✅ customer_id: Invalid_texts defined
  ⚠ Consider adding valid_regexp for customer_id for better validation

Tables Section (2 tables):
  ✅ customer_profiles: 2 key columns mapped
  ✅ orders: 1 key column mapped
  ✅ All referenced keys exist

Canonical IDs Section:
  ✅ Name: unified_id
  ✅ Merge keys: email, customer_id (both exist)
  ✅ Iterations: 15 (recommended range: 10-20)

Master Tables Section (1 master table):
  ✅ customer_master: References unified_id
  ✅ Attribute 'best_email': 2 sources with priorities
  ✅ All source tables exist

Cross-References:
  ✅ All merge_by_keys defined in keys section
  ✅ All key_columns reference existing keys
  ✅ All master table sources exist
  ✅ No canonical ID name conflicts

Recommendations:
  💡 Consider adding valid_regexp for customer_id (e.g., "^[A-Z0-9]+$")
  💡 Add more master table attributes for richer customer profiles
  💡 Consider array attributes (top_3_emails) for historical tracking

Summary:
  ✅ 0 errors
  ⚠ 1 warning
  💡 3 recommendations

✓ Configuration is ready for SQL generation!
```

---

## Validation Checks

### Required Checks (Must Pass)
- [ ] File exists and is readable
- [ ] Valid YAML syntax
- [ ] `name` field present
- [ ] `keys` section present with at least one key
- [ ] `tables` section present with at least one table
- [ ] `canonical_ids` section present
- [ ] All merge_by_keys exist in keys section
- [ ] All key_columns reference defined keys
- [ ] No duplicate key names
- [ ] No duplicate table names

### Warning Checks (Recommended)
- [ ] Keys have validation rules (valid_regexp or invalid_texts)
- [ ] Merge_iterations specified (otherwise auto-calculated)
- [ ] Master tables defined for unified customer view
- [ ] Source tables have unique key combinations
- [ ] Attribute priorities are sequential

### Best Practice Checks
- [ ] Email keys have email regex pattern
- [ ] Phone keys have phone validation
- [ ] Invalid_texts include common null values ('', 'N/A', 'null')
- [ ] Master tables use time-based order_by for recency
- [ ] Array attributes for historical data (top_3_emails, etc.)

---

## Common Validation Errors

### Syntax Errors
**Error**: `Invalid YAML: mapping values are not allowed here`
**Solution**: Check indentation (use spaces, not tabs), ensure colons have space after them

**Error**: `Invalid YAML: could not find expected ':'`
**Solution**: Check for missing colons in key-value pairs

### Structure Errors
**Error**: `Missing required section: keys`
**Solution**: Add keys section with at least one key definition

**Error**: `Empty tables section`
**Solution**: Add at least one table with key_columns

### Reference Errors
**Error**: `Key 'phone' referenced in table 'orders' but not defined in keys section`
**Solution**: Add phone key to keys section or remove reference

**Error**: `Merge key 'phone_number' not found in keys section`
**Solution**: Add phone_number to keys section or remove from merge_by_keys

**Error**: `Master table source 'customer_360' not found in tables section`
**Solution**: Add customer_360 to tables section or use correct table name

### Value Errors
**Error**: `merge_iterations must be a positive integer, got: 'auto'`
**Solution**: Either remove merge_iterations (auto-calculate) or specify integer (e.g., 15)

**Error**: `Priority must be a positive integer, got: 'high'`
**Solution**: Use numeric priority (1 for highest, 2 for second, etc.)

---

## Validation Levels

### Strict Mode (Default)
- Fails on any structural errors
- Warns on missing best practices
- Recommends optimizations

### Lenient Mode
- Only fails on critical syntax errors
- Allows missing optional fields
- Minimal warnings

---

## Platform-Specific Validation

### Databricks-Specific
- ✓ Table names compatible with Unity Catalog
- ✓ Column names valid for Spark SQL
- ⚠ Check for reserved keywords (DATABASE, TABLE, etc.)

### Snowflake-Specific
- ✓ Table names compatible with Snowflake
- ✓ Column names valid for Snowflake SQL
- ⚠ Check for reserved keywords (ACCOUNT, SCHEMA, etc.)

---

## What Happens Next

### If Validation Passes
```
✅ Configuration validated successfully!

Ready for:
  • SQL generation (Databricks or Snowflake)
  • Direct execution after generation

Next steps:
  1. /cdp-hybrid-idu:hybrid-generate-databricks
  2. /cdp-hybrid-idu:hybrid-generate-snowflake
  3. /cdp-hybrid-idu:hybrid-setup (complete workflow)
```

### If Validation Fails
```
❌ Configuration has errors that must be fixed

Errors (must fix):
  1. Missing required section: canonical_ids
  2. Undefined key 'phone' referenced in table 'orders'

Suggestions:
  • Add canonical_ids section with name and merge_by_keys
  • Add phone key to keys section or remove from orders

Would you like help fixing these issues? (y/n)
```

I can help you:
- Fix syntax errors
- Add missing sections
- Define proper validation rules
- Optimize configuration

---

## Success Criteria

Validation passes when:
- ✅ YAML syntax is valid
- ✅ All required sections present
- ✅ All references resolved
- ✅ No structural errors
- ✅ Ready for SQL generation

---

**Ready to validate your YAML configuration?**

Provide your `unify.yml` file path to begin validation!
