---
name: new-ingestion
description: Create a complete ingestion workflow for a new data source
---

# Create New Ingestion Workflow

## ⚠️ CRITICAL: This command enforces strict template adherence

I'll help you create a production-ready CDP ingestion workflow following exact templates from documentation.

---

## Required Information

Please provide the following details:

### 1. Data Source
- **Source Name**: What system are we ingesting from? (e.g., Salesforce, HubSpot, Custom API)
- **Connector Type**: What TD connector does it use? (e.g., `salesforce`, `hubspot`, `rest`)

### 2. Tables/Objects
- **Object Names**: Which tables or objects need to be ingested? (comma-separated)
- **Table Names**: Desired table names in Treasure Data

### 3. Ingestion Mode
- **Mode**: Choose one:
  - `incremental` - Ongoing data sync only
  - `historical` - One-time historical backfill only
  - `both` - Separate historical and incremental workflows

### 4. Incremental Logic (if applicable)
- **Incremental Field**: Field indicating record updates (e.g., `updated_at`, `modified_date`)
- **Default Start Date**: Initial load start date (format: `2023-09-01T00:00:00.000000`)

### 5. Authentication
- **Credentials Needed**: What credentials are required?
  - API keys?
  - OAuth tokens?
  - Service account keys?
  - Username/password?

### 6. Database Target
- **Target Database**: Which Treasure Data database? (default: `mck_src`)

---

## What I'll Do

### Step 1: Read Documentation (MANDATORY)
I will READ the following documentation BEFORE generating ANY code:
- `docs/sources/template-new-source.md` - Template for new sources
- `docs/patterns/workflow-patterns.md` - Workflow structures
- `docs/patterns/logging-patterns.md` - Logging templates
- `docs/patterns/timestamp-formats.md` - Timestamp functions
- `docs/patterns/incremental-patterns.md` - Incremental patterns

### Step 2: Generate ALL Files in ONE Response
I will create all required files in a SINGLE response:
- Complete `.dig` workflow file(s)
- Datasource configuration YAML
- Individual table load YAML files
- All files created together for consistency

### Step 3: Use Exact Templates
I will copy templates character-for-character from documentation:
- No simplification
- No optimization
- No "improvements"
- Only replace placeholders: `{source_name}`, `{object_name}`, `{database}`

### Step 4: Verify Quality Gates
Before delivering, I will verify:
✅ All template sections present
✅ All logging blocks included (start, success, error)
✅ All error handling blocks present
✅ Timestamp format correct for connector
✅ Incremental field handling correct
✅ No deviations from documented templates

---

## Output

I will generate:

### For Incremental-Only Mode:
1. `ingestion/{source}_ingest_inc.dig` - Incremental workflow
2. `ingestion/config/{source}_datasources.yml` - Datasource configuration
3. `ingestion/config/{source}_{object1}_load.yml` - Load config for object 1
4. `ingestion/config/{source}_{object2}_load.yml` - Load config for object 2 (if multiple objects)
... and so on

### For Historical + Incremental Mode:
1. `ingestion/{source}_ingest_hist.dig` - Historical backfill workflow
2. `ingestion/{source}_ingest_inc.dig` - Incremental workflow
3. `ingestion/config/{source}_datasources.yml` - Datasource configuration
4. `ingestion/config/{source}_{object1}_load.yml` - Load config for object 1
... and so on

### Plus:
- Updated `credentials_ingestion.json` template with required credentials
- Deployment instructions
- Testing steps

---

## Next Steps After Generation

1. **Upload credentials**:
   ```bash
   cd ingestion
   td wf secrets --project ingestion --set @credentials_ingestion.json
   ```

2. **Test syntax**:
   ```bash
   td wf check {source}_ingest_inc.dig
   ```

3. **Run workflow**:
   ```bash
   td wf run {source}_ingest_inc.dig
   ```

4. **Monitor ingestion log**:
   ```sql
   SELECT * FROM mck_src.ingestion_log
   WHERE source_name = '{source}'
   ORDER BY time DESC
   LIMIT 10
   ```

---

## Production-Ready Guarantee

All generated code will:
- ✅ Work the first time
- ✅ Follow consistent patterns
- ✅ Include complete error handling
- ✅ Include comprehensive logging
- ✅ Be maintainable and documented
- ✅ Match production standards exactly

---

**Ready to proceed? Please provide the required information above and I'll generate your complete ingestion workflow using exact templates from documentation.**
