# Interactive Configuration Collection Guide

**Version**: 1.0.0
**Purpose**: Universal pattern for all CDP plugins to collect configuration interactively
**Applies To**: All CDP plugin agents (ingestion, hist-union, staging, unification)

---

## 🎯 CORE PRINCIPLE

**ALWAYS** collect configuration one question at a time in a conversational manner.
**NEVER** ask for all information upfront in a bulk questionnaire format.

---

## Universal Rules

### Rule 1: Check Existing Input
```
IF user_message contains ALL required parameters → Skip to execution
ELSE IF user_message contains SOME parameters → Ask for missing ones only
ELSE → Begin full interactive collection
```

### Rule 2: One Question at a Time
**WAIT for user response before asking next question.**

✅ **CORRECT**: Ask one, wait, ask next
❌ **WRONG**: Ask for everything in a list

### Rule 3: Offer Intelligent Defaults
Always suggest sensible defaults in parentheses.

**Example**: "What's the target database? (default: `mck_src`)"

---

## PLUGIN 1: CDP-Ingestion

### Required Parameters
1. **source_name** - Data source (e.g., Snowflake, Salesforce)
2. **connector_type** - TD connector (e.g., `snowflake`, `salesforce`)
3. **tables** - Comma-separated tables/objects
4. **mode** - `incremental`, `historical`, or `both`
5. **target_database** - TD database (default: `mck_src`)

### Conditional Parameters
**IF mode = incremental OR both**:
- **incremental_field** - Tracking column (e.g., `updated_at`)
- **default_start_date** - Format: `2023-01-01T00:00:00.000000`

**IF connector needs auth**:
- **auth_id** - TD authentication ID

---

## PLUGIN 2: CDP-Hist-Union

### Required Parameters
1. **table_name** - Base table (e.g., `mck_src.klaviyo_events`)
2. **lookup_database** - Watermark DB (default: `mck_references`)

---

## PLUGIN 3: CDP-Staging

### Required Parameters
1. **table_list** - Comma-separated tables
2. **source_database** - Source DB (e.g., `mck_src`)
3. **staging_database** - Target DB (default: `mck_stg`)
4. **lookup_database** - Reference DB (default: `mck_references`)
5. **sql_engine** - `presto` (default) or `hive`

---

## PLUGIN 4: CDP-Unification

### Required Parameters
1. **client_short_name** - Client identifier (e.g., `ik_claude`, `mck`)
2. **name** - Unification name (e.g., `customer_360`)
3. **id_method** - `persistent_id` or `canonical_id`
4. **update_strategy** - `incremental` or `full`
5. **tables** - Comma-separated staging tables
6. **regional_endpoint** - TD API endpoint
7. **lookup_database** - References database suffix (default: `references`)
   - Creates: `${client_short_name}_${lookup_database}` (e.g., `ik_claude_references`)
   - **MUST CREATE** this database before workflow execution

---

## ✅ IMPLEMENTATION CHECKLIST

When implementing interactive configuration in your agent:

- [ ] Check if user provided ANY parameters already
- [ ] Ask ONE question at a time
- [ ] Wait for response before next question
- [ ] Offer defaults for optional parameters
- [ ] Ask conditional questions only when relevant
- [ ] Summarize all collected config before execution
- [ ] Confirm with user before proceeding

---

**Remember**: Interactive configuration improves user experience and reduces errors!
