---
name: unify-validate
description: Validate all ID unification files against exact templates before deployment
---

# ID Unification Validation Command

## Purpose

**MANDATORY validation gate** that checks ALL generated unification files against exact templates from agent prompts. This prevents deployment of incorrect configurations.

**⚠️ CRITICAL**: This command MUST complete successfully before `td wf push` or workflow execution.

---

## What This Command Validates

### 1. File Existence Check
- ✅ `unification/unif_runner.dig` exists
- ✅ `unification/dynmic_prep_creation.dig` exists
- ✅ `unification/id_unification.dig` exists
- ✅ `unification/enrich_runner.dig` exists
- ✅ `unification/config/environment.yml` exists
- ✅ `unification/config/src_prep_params.yml` exists
- ✅ `unification/config/unify.yml` exists
- ✅ `unification/config/stage_enrich.yml` exists
- ✅ All SQL files in `unification/queries/` exist
- ✅ All SQL files in `unification/enrich/queries/` exist

### 2. Template Compliance Check

**unif_runner.dig Validation:**
- ✅ Uses `require>` operator (NOT `call>`)
- ✅ No `echo>` operators with subtasks
- ✅ Matches exact template from `/plugins/cdp-unification/prompt.md` lines 186-217
- ✅ Has `_error:` section with email_alert
- ✅ Includes both `config/environment.yml` and `config/src_prep_params.yml`

**stage_enrich.yml Validation:**
- ✅ RULE 1: `unif_input` table has `column` and `key` both using `alias_as`
- ✅ RULE 2: Staging tables have `column` using `col.name` and `key` using `alias_as`
- ✅ All key_columns match actual columns from `src_prep_params.yml`
- ✅ No template columns (like adobe_clickstream, loyalty_id_std)
- ✅ Table names match `src_tbl` (NO _prep suffix)

**enrich_runner.dig Validation:**
- ✅ Matches exact template from `unification-staging-enricher.md` lines 261-299
- ✅ Includes all 3 config files in `_export`
- ✅ Uses `td_for_each>` for dynamic execution
- ✅ Has Presto and Hive conditional execution

### 3. Database & Table Existence Check
- ✅ `${client_short_name}_${src}` database exists
- ✅ `${client_short_name}_${stg}` database exists
- ✅ `${client_short_name}_${gld}` database exists (if used)
- ✅ `${client_short_name}_${lkup}` database exists
- ✅ `cdp_unification_${unif_name}` database exists
- ✅ `${client_short_name}_${lkup}.exclusion_list` table exists

### 4. Configuration Validation
- ✅ All variables in `environment.yml` are defined
- ✅ All tables in `src_prep_params.yml` exist in source database
- ✅ All columns in `src_prep_params.yml` exist in source tables
- ✅ `unify.yml` merge_by_keys match `src_prep_params.yml` alias_as columns
- ✅ No undefined variables (${...})

### 5. YAML Syntax Check
- ✅ All YAML files have valid syntax
- ✅ Proper indentation (2 spaces)
- ✅ No tabs in YAML files
- ✅ All strings properly quoted where needed

---

## Validation Report Format

```
╔══════════════════════════════════════════════════════════════╗
║          ID UNIFICATION VALIDATION REPORT                    ║
╚══════════════════════════════════════════════════════════════╝

[1/5] File Existence Check
  ✅ unification/unif_runner.dig
  ✅ unification/dynmic_prep_creation.dig
  ✅ unification/id_unification.dig
  ✅ unification/enrich_runner.dig
  ✅ unification/config/environment.yml
  ✅ unification/config/src_prep_params.yml
  ✅ unification/config/unify.yml
  ✅ unification/config/stage_enrich.yml
  ✅ 3/3 SQL files in queries/
  ✅ 4/4 SQL files in enrich/queries/

[2/5] Template Compliance Check
  ✅ unif_runner.dig uses require> operator
  ✅ unif_runner.dig has no echo> conflicts
  ✅ stage_enrich.yml RULE 1 compliant (unif_input table)
  ✅ stage_enrich.yml RULE 2 compliant (staging tables)
  ❌ stage_enrich.yml has incorrect mapping on line 23
      Expected: column: email_address_std
      Found:    column: email
      FIX: Update line 23 to use col.name from src_prep_params.yml

[3/5] Database & Table Existence
  ✅ client_src exists
  ✅ client_stg exists
  ✅ client_gld exists
  ✅ client_config exists
  ❌ client_config.exclusion_list does NOT exist
      FIX: Run: td query -d client_config -t presto -w "CREATE TABLE IF NOT EXISTS exclusion_list (key_value VARCHAR, key_name VARCHAR, tbls ARRAY(VARCHAR), note VARCHAR)"

[4/5] Configuration Validation
  ✅ All variables defined in environment.yml
  ✅ Source table client_stg.snowflake_orders exists
  ✅ All columns exist in source table
  ✅ unify.yml keys match src_prep_params.yml

[5/5] YAML Syntax Check
  ✅ All YAML files have valid syntax
  ✅ Proper indentation
  ✅ No tabs found

╔══════════════════════════════════════════════════════════════╗
║                    VALIDATION SUMMARY                        ║
╚══════════════════════════════════════════════════════════════╝

Total Checks: 45
Passed: 43 ✅
Failed: 2 ❌

❌ VALIDATION FAILED - DO NOT DEPLOY

Required Actions:
1. Fix stage_enrich.yml line 23 mapping
2. Create client_config.exclusion_list table

Re-run validation after fixes: /cdp-unification:unify-validate
```

---

## Error Codes

- **EXIT 0**: All validations passed ✅
- **EXIT 1**: File existence failures
- **EXIT 2**: Template compliance failures
- **EXIT 3**: Database/table missing
- **EXIT 4**: Configuration errors
- **EXIT 5**: YAML syntax errors

---

## Usage

**Standalone:**
```
/cdp-unification:unify-validate
```

**Auto-triggered in unify-setup** (MANDATORY step before deployment)

**Manual validation before deployment:**
```
cd unification
/cdp-unification:unify-validate
```

If validation PASSES → Proceed with `td wf push unification`
If validation FAILS → Fix errors and re-validate

---

## Integration with unify-setup

The `/unify-setup` command will automatically:
1. Generate all unification files
2. **RUN VALIDATION** (this command)
3. **BLOCK deployment** if validation fails
4. **Show fix instructions** for each error
5. **Auto-retry validation** after fixes
6. Only proceed to deployment after 100% validation success

---

## Success Criteria

✅ **ALL checks must pass** before deployment is allowed
✅ **No exceptions** - even 1 failure blocks deployment
✅ **Detailed error messages** with exact fix instructions
✅ **Auto-remediation suggestions** where possible

---

**Let's validate your unification files!**
