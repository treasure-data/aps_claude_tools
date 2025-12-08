---
name: ingest-new
description: Create a complete ingestion workflow for a new data source
---

# Create New Ingestion Workflow

## ⚠️ MANDATORY: Interactive Configuration Collection

**YOU MUST use `AskUserQuestion` tool to collect parameters interactively.**

### Configuration Steps (FOLLOW IN ORDER)

**Step 1: Check if user already provided parameters**
- IF all parameters present in user message → Skip to Step 2
- ELSE → Collect missing parameters using AskUserQuestion (one at a time)

**Step 2: Collect Required Parameters** (use AskUserQuestion tool for each):

1. **Source Name & Connector Type**
   - Ask: "What data source are you ingesting from?"
   - Options: Klaviyo, Shopify, Salesforce, BigQuery, Snowflake, Custom API
   - Infer connector type from source (e.g., klaviyo → rest, salesforce → salesforce)

2. **Ingestion Mode**
   - Ask: "What ingestion mode do you need?"
   - Options:
     - "Both (historical + incremental)" [recommended]
     - "Incremental only"
     - "Historical only"

3. **Tables/Objects**
   - Ask: "Please provide tables/objects to ingest (comma-separated, e.g., `orders,customers,products`)"

4. **Incremental Field** (ONLY if mode = incremental OR both)
   - Ask: "What field tracks record updates? (e.g., `updated_at`, `modified_date`)"

5. **Start Date** (ONLY if mode = incremental OR both)
   - Ask: "What is the initial load start date? Format: `YYYY-MM-DDTHH:mm:ss.000000`"

6. **Target Database**
   - Ask: "Which target database?"
   - Options: "Use {client}_src (default)", "Use mck_src", "Custom database"

7. **Authentication**
   - Ask: "What credentials are needed? (e.g., API Key, OAuth Token, Service Account JSON)"

**Step 3: Show Configuration Summary**

After collecting all parameters, display:
```
📋 Configuration Summary:
Source: {source_name}
Connector: {connector_type}
Mode: {mode}
Objects: {objects}
Target Database: {target_database}
[Incremental Field: {field}]
[Start Date: {date}]

Proceed with workflow generation? (yes/no)
```

WAIT for user confirmation.

---

## Workflow Generation Process

### Step 1: Read Documentation (MANDATORY - DO NOT SKIP)

**Execute this command FIRST to find plugin directory:**
```bash
find ~/.claude -name "cdp-ingestion" -type d
```

**Then use Read tool on these files from the plugin directory:**
1. `docs/patterns/workflow-patterns.md`
2. `docs/patterns/logging-patterns.md`
3. `docs/patterns/timestamp-formats.md`
4. `docs/sources/google-bigquery.md` (for BigQuery) OR `docs/sources/klaviyo.md` (for Klaviyo) OR similar source doc
5. **ACTUAL EXAMPLE WORKFLOWS** - Read existing `.dig` and `.yml` files for the connector type

**You MUST use Read tool on actual files. DO NOT generate from memory.**

### Step 2: Generate ALL Files in ONE Response
Create all required files using multiple Write tool calls in SINGLE response:
- Workflow file(s): `.dig` files
- Datasource config: `config/{source}_datasources.yml`
- Load configs: `config/{source}_{object}_load.yml` (one per object)

### Step 3: Use Exact Templates (COPY, DO NOT CREATE)
Copy templates character-for-character from the files you read in Step 1:
- **COPY from actual .dig files you read** - do NOT create new workflows
- **COPY from actual .yml files you read** - do NOT generate new configs
- **COPY from actual .sql files you read** - do NOT write new SQL
- No simplification, no optimization, no "improvements"
- Only replace: source names, table names, database names, auth IDs, dates

### Step 4: Verify Quality Gates
Before delivering, verify:
✅ All template sections present
✅ All logging blocks included (start, success, error)
✅ All error handling blocks present
✅ Timestamp format correct for connector
✅ Incremental field handling correct

---

## Output Files

### For Incremental-Only Mode:
- `ingestion/{source}_ingest_inc.dig`
- `ingestion/config/{source}_datasources.yml`
- `ingestion/config/{source}_{object}_load.yml` (per object)

### For Historical + Incremental Mode:
- `ingestion/{source}_ingest_hist.dig`
- `ingestion/{source}_ingest_inc.dig`
- `ingestion/config/{source}_datasources.yml`
- `ingestion/config/{source}_{object}_load.yml` (per object)

---

## Next Steps (Show to User)

After successful generation:

1. **Upload credentials**:
   ```bash
   cd ingestion
   td wf secrets --project ingestion --set @credentials_ingestion.json
   ```

2. **Test syntax**:
   ```bash
   td wf check {source}_ingest_inc.dig
   ```

3. **Deploy workflow**:
   ```bash
   td wf push ingestion
   ```

4. **Run workflow**:
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

**Ready! Start by asking the first configuration question using AskUserQuestion tool.**
