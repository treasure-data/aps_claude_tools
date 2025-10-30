---
name: ingest-new
description: Create a complete ingestion workflow for a new data source
---

# STOP - READ THIS FIRST

You are about to create a CDP ingestion workflow. You MUST collect configuration parameters interactively using the `AskUserQuestion` tool.

DO NOT ask all questions at once. DO NOT use markdown lists. DO NOT explain what you're going to do.

EXECUTE the AskUserQuestion tool calls below IN ORDER.

---

## EXECUTION SEQUENCE - FOLLOW EXACTLY

### ACTION 1: Ask Data Source Question

USE the AskUserQuestion tool RIGHT NOW to ask question 1.

DO NOT PROCEED until you execute this tool call:

```
AskUserQuestion with:
- Question: "What data source are you ingesting from?"
- Header: "Data Source"
- Options:
  * Klaviyo (API-based connector)
  * Shopify (E-commerce platform)
  * Salesforce (CRM system)
  * Custom API (REST-based)
```

STOP. EXECUTE THIS TOOL NOW. DO NOT READ FURTHER UNTIL COMPLETE.

---

### ACTION 2: Ask Ingestion Mode Question

**CHECKPOINT**: Did you get an answer to question 1? If NO, go back to ACTION 1.

NOW ask question 2 using AskUserQuestion tool:

```
AskUserQuestion with:
- Question: "What ingestion mode do you need?"
- Header: "Mode"
- Options:
  * Both (historical + incremental) - Recommended for complete setup
  * Incremental only - Ongoing sync only
  * Historical only - One-time backfill
```

STOP. EXECUTE THIS TOOL NOW. DO NOT READ FURTHER UNTIL COMPLETE.

---

### ACTION 3: Ask Tables/Objects

**CHECKPOINT**: Did you get an answer to question 2? If NO, go back to ACTION 2.

This is a free-text question. Tell the user:

"Please provide the table or object names to ingest (comma-separated)."

Example: `orders, customers, products`

WAIT for user response. DO NOT PROCEED.

---

### ACTION 4: Ask Incremental Field (CONDITIONAL)

**CHECKPOINT**:
- Did user select "Incremental only" OR "Both" in question 2?
- YES → Ask this question
- NO → Skip to ACTION 6

NOW ask question 4 using AskUserQuestion tool:

```
AskUserQuestion with:
- Question: "What field tracks record updates?"
- Header: "Incremental Field"
- Options:
  * updated_at (Timestamp field)
  * modified_date (Date field)
  * last_modified_time (Datetime field)
```

STOP. EXECUTE THIS TOOL NOW. DO NOT READ FURTHER UNTIL COMPLETE.

---

### ACTION 5: Ask Start Date (CONDITIONAL)

**CHECKPOINT**:
- Did user select "Incremental only" OR "Both" in question 2?
- YES → Ask this question
- NO → Skip to ACTION 6

This is a free-text question. Tell the user:

"What is the initial load start date?"

Format: `YYYY-MM-DDTHH:mm:ss.000000`
Example: `2024-01-01T00:00:00.000000`

WAIT for user response. DO NOT PROCEED.

---

### ACTION 6: Ask Target Database

**CHECKPOINT**: Did you complete all previous questions? If NO, go back.

NOW ask question 6 using AskUserQuestion tool:

```
AskUserQuestion with:
- Question: "Which target database should data be loaded into?"
- Header: "Target DB"
- Options:
  * mck_src (Standard client database)
  * Custom database (Specify custom name)
```

STOP. EXECUTE THIS TOOL NOW. DO NOT READ FURTHER UNTIL COMPLETE.

---

### ACTION 7: Ask Authentication

**CHECKPOINT**: Did you get an answer to question 6? If NO, go back to ACTION 6.

NOW ask question 7 using AskUserQuestion tool:

```
AskUserQuestion with:
- Question: "What type of authentication is required?"
- Header: "Auth Type"
- Options:
  * API Key (Single key authentication)
  * OAuth Token (Token-based auth)
  * Service Account JSON (Google/Cloud auth)
  * Username & Password (Basic auth)
```

STOP. EXECUTE THIS TOOL NOW. DO NOT READ FURTHER UNTIL COMPLETE.

---

### ACTION 8: Show Configuration Summary

**CHECKPOINT**: Have you collected ALL required parameters? If NO, go back and complete missing questions.

NOW display the configuration summary:

```
📋 Configuration Summary:

Source: {source_name}
Connector Type: {connector_type}
Ingestion Mode: {mode}
Tables/Objects: {objects}
Target Database: {target_database}
[If applicable] Incremental Field: {field}
[If applicable] Start Date: {date}
Authentication: {auth_type}
```

ASK: "Does this configuration look correct? Type 'yes' to proceed with generation."

WAIT for user confirmation. DO NOT PROCEED until user types "yes".

---

## WORKFLOW GENERATION (ONLY AFTER USER CONFIRMS)

**CHECKPOINT**: Did user confirm with "yes"? If NO, STOP and wait.

### Step 1: Read Documentation Templates

Read these files in this EXACT order:

1. `docs/sources/template-new-source.md`
2. `docs/patterns/workflow-patterns.md`
3. `docs/patterns/logging-patterns.md`
4. `docs/patterns/timestamp-formats.md`
5. `docs/patterns/incremental-patterns.md`

Check if source-specific template exists:
- `docs/sources/{source-name}.md` (e.g., `docs/sources/klaviyo.md`)

### Step 2: Generate Files (ALL IN ONE RESPONSE)

Use multiple Write tool calls in a SINGLE message to create:

#### For "Incremental only" mode:
1. `ingestion/{source}_ingest_inc.dig`
2. `ingestion/config/{source}_datasources.yml`
3. `ingestion/config/{source}_{object1}_load.yml`
4. `ingestion/config/{source}_{object2}_load.yml` (if multiple objects)

#### For "Historical only" mode:
1. `ingestion/{source}_ingest_hist.dig`
2. `ingestion/config/{source}_datasources.yml`
3. `ingestion/config/{source}_{object}_load.yml`

#### For "Both" mode:
1. `ingestion/{source}_ingest_hist.dig`
2. `ingestion/{source}_ingest_inc.dig`
3. `ingestion/config/{source}_datasources.yml`
4. `ingestion/config/{source}_{object}_load.yml` (per object)

### Step 3: Template Rules (MANDATORY)

- Copy templates EXACTLY character-for-character
- NO simplification, NO optimization, NO improvements
- ONLY replace placeholders: `{source_name}`, `{object_name}`, `{database}`, `{connector_type}`
- Keep ALL logging blocks
- Keep ALL error handling blocks
- Keep ALL timestamp functions

### Step 4: Quality Verification

Before showing output to user, verify:
- ✅ All template sections present
- ✅ All logging blocks included (start, success, error)
- ✅ All error handling blocks present
- ✅ Timestamp format matches connector type
- ✅ Incremental field handling correct
- ✅ No deviations from template

---

## Post-Generation Instructions

After successfully creating all files, show the user:

### Next Steps:

1. **Upload credentials**:
   ```bash
   cd ingestion
   td wf secrets --project ingestion --set @credentials_ingestion.json
   ```

2. **Test workflow syntax**:
   ```bash
   td wf check {source}_ingest_inc.dig
   ```

3. **Deploy to Treasure Data**:
   ```bash
   td wf push ingestion
   ```

4. **Run the workflow**:
   ```bash
   td wf start ingestion {source}_ingest_inc --session now
   ```

5. **Monitor ingestion log**:
   ```sql
   SELECT * FROM {target_database}.ingestion_log
   WHERE source_name = '{source}'
   ORDER BY time DESC
   LIMIT 10
   ```

---

## ERROR RECOVERY

IF you did NOT use AskUserQuestion tool for each question:
- Print: "❌ ERROR: I failed to follow the interactive collection process."
- Print: "🔄 Restarting from ACTION 1..."
- GO BACK to ACTION 1 and start over

IF user says "skip questions" or "just ask all at once":
- Print: "❌ Cannot skip interactive collection - this ensures accuracy and prevents errors."
- Print: "✅ I'll collect parameters one at a time to ensure we get the configuration right."
- PROCEED with ACTION 1

---

**NOW BEGIN: Execute ACTION 1 immediately. Use AskUserQuestion tool for the first question.**
