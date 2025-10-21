---
name: ingest-add-klaviyo
description: Generate complete Klaviyo ingestion workflow with all data sources using exact templates
---

# Add Klaviyo Ingestion

## ⚠️ CRITICAL: This command generates ALL files at ONCE using exact templates

I'll create a complete Klaviyo ingestion setup based on proven templates from `docs/sources/klaviyo.md`.

---

## What I'll Generate

### MANDATORY: All files created in ONE response

I will generate ALL of the following files in a SINGLE response using multiple Write tool calls:

### Workflow Files
1. **`ingestion/klaviyo_ingest_inc.dig`** - Incremental ingestion workflow
2. **`ingestion/klaviyo_ingest_hist.dig`** - Historical backfill workflow

### Configuration Files (in `ingestion/config/`)
3. **`klaviyo_datasources.yml`** - Datasource definitions for all objects
4. **`klaviyo_profiles_load.yml`** - Profiles configuration
5. **`klaviyo_events_load.yml`** - Events configuration
6. **`klaviyo_campaigns_load.yml`** - Campaigns configuration
7. **`klaviyo_lists_load.yml`** - Lists configuration
8. **`klaviyo_email_templates_load.yml`** - Email templates configuration
9. **`klaviyo_metrics_load.yml`** - Metrics configuration

### Credentials Template
10. Updated `credentials_ingestion.json` with Klaviyo credentials section

**Total: 10 files created in ONE response**

---

## Prerequisites

Please provide the following information:

### Required
1. **Klaviyo API Key**: Your Klaviyo private API key (will be stored as secret)
2. **TD Authentication ID**: Treasure Data authentication ID for Klaviyo connector (e.g., `klaviyo_auth_default`)
3. **Default Start Date**: Initial historical load start date
   - Format: `YYYY-MM-DDTHH:MM:SS.000000`
   - Example: `2023-09-01T00:00:00.000000`

### Optional
4. **Target Database**: Default is `client_src` (leave blank to use default)

---

## Process I'll Follow

### Step 1: Read Klaviyo Documentation (MANDATORY)
I will READ these documentation files BEFORE generating ANY code:
- `docs/sources/klaviyo.md` - Klaviyo exact templates
- `docs/patterns/workflow-patterns.md` - Workflow patterns
- `docs/patterns/logging-patterns.md` - Logging templates
- `docs/patterns/timestamp-formats.md` - Klaviyo timestamp format (`.000000`)
- `docs/patterns/incremental-patterns.md` - Dual field names for campaigns

### Step 2: Generate ALL 10 Files in ONE Response
Using multiple Write tool calls in a SINGLE message:
- Write workflow files (2 files)
- Write datasource config (1 file)
- Write load configs (6 files)
- Write credentials template update (1 file)

### Step 3: Copy Exact Templates
Templates will be copied character-for-character from documentation:
- Klaviyo-specific timestamp format: `.000000` (6 decimals, NO Z)
- Dual incremental fields for campaigns: `updated_at` in table, `updated` in API
- Events with NO incremental field parameter
- Exact SQL logging blocks
- Exact error handling blocks

### Step 4: Verify Quality Gates
Before delivering, I will verify:
✅ All 10 files created
✅ Klaviyo timestamp format: `.000000` (6 decimals, NO Z)
✅ Campaigns dual field names correct
✅ Events config has NO incremental_field parameter
✅ All logging blocks present (start, success, error)
✅ All error handling blocks present
✅ Parallel processing with limit: 3
✅ COALESCE fallback to historical table

---

## Klaviyo-Specific Configuration

### Objects Included
1. **Profiles**: Customer profiles (incremental: `updated`)
2. **Events**: Customer events (NO incremental field)
3. **Campaigns**: Email campaigns (incremental: `updated_at` in table, `updated` in API)
4. **Lists**: Email lists (incremental: `updated`)
5. **Email Templates**: Campaign templates (incremental: `updated`)
6. **Metrics**: Event metrics (incremental: `updated`)

### Key Features
- **Dual incremental fields**: Campaigns use different field names in table vs API
- **Events handling**: No incremental parameter in config
- **Timestamp format**: `.000000` (6 decimals, NO Z suffix)
- **Parallel processing**: Limit of 3 for API rate limits
- **Fallback logic**: COALESCE from incremental → historical → default

---

## After Generation

### 1. Upload Credentials
```bash
# Navigate to your ingestion directory
cd ingestion/
td wf secrets --project ingestion --set @credentials_ingestion.json
```

### 2. Test Syntax
```bash
td wf check klaviyo_ingest_inc.dig
td wf check klaviyo_ingest_hist.dig
```

### 3. Run Historical Backfill (First Time)
```bash
td wf run klaviyo_ingest_hist.dig
```

### 4. Run Incremental (Ongoing)
```bash
td wf run klaviyo_ingest_inc.dig
```

### 5. Monitor Ingestion
```sql
SELECT * FROM client_src.ingestion_log
WHERE source_name LIKE 'klaviyo%'
ORDER BY time DESC
LIMIT 20
```

---

## Production-Ready Guarantee

All generated code will:
- ✅ Follow exact Klaviyo templates from `docs/sources/klaviyo.md`
- ✅ Use correct timestamp format (`.000000`)
- ✅ Handle dual incremental fields correctly
- ✅ Include all 6 data objects
- ✅ Include comprehensive logging and error handling
- ✅ Work the first time without modifications

---

**Ready to proceed? Provide the required information (API key, TD auth ID, start date) and I'll generate all 10 files in ONE response using exact templates from documentation.**
