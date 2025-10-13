---
name: ingest-add-object
description: Add a new object/table to an existing data source workflow
---

# Add Object to Existing Source

## ⚠️ CRITICAL: This command generates ALL required files at ONCE

I'll help you add a new object/table to an existing ingestion source following exact templates from documentation.

---

## Required Information

Please provide:

### 1. Source Information
- **Existing Source Name**: Which source? (e.g., `klaviyo`, `shopify_v2`, `salesforce`)
- **New Object Name**: What object are you adding? (e.g., `orders`, `products`, `contacts`)

### 2. Object Details
- **Table Name**: Desired table name in Treasure Data (e.g., `shopify_orders`)
- **Incremental Field**: Field indicating record updates (e.g., `updated_at`, `modified_date`)
- **Default Start Date**: Initial load start date (format: `2023-09-01T00:00:00.000000`)

### 3. Ingestion Mode
- **Mode**: Which workflow?
  - `incremental` - Add to incremental workflow
  - `historical` - Add to historical workflow
  - `both` - Add to both workflows

---

## What I'll Do

### MANDATORY: All files created/updated in ONE response

I will generate/update ALL of the following in a SINGLE response:

#### Files to Create
1. **`ingestion/config/{source}_{object}_load.yml`** - New load configuration

#### Files to Update
2. **`ingestion/config/{source}_datasources.yml`** - Add object to datasource list
3. **`ingestion/{source}_ingest_inc.dig`** - Updated workflow (if incremental mode)
4. **`ingestion/{source}_ingest_hist.dig`** - Updated workflow (if historical mode)

**Total: 1 new file + 2-3 updated files in ONE response**

---

## Process I'll Follow

### Step 1: Read Source Documentation (MANDATORY)
I will READ the source-specific documentation BEFORE making ANY changes:
- `docs/sources/{source}.md` - Source-specific exact templates
- `docs/patterns/timestamp-formats.md` - Correct timestamp format
- `docs/patterns/incremental-patterns.md` - Incremental field handling

### Step 2: Read Existing Files
I will read the existing workflow and datasource config to understand current structure

### Step 3: Generate/Update ALL Files in ONE Response
Using multiple Write/Edit tool calls in a SINGLE message:
- Write new load config
- Edit datasource config to add new object
- Edit workflow(s) to include new object processing

### Step 4: Copy Exact Templates
I will use exact templates for the new object:
- Match existing object patterns exactly
- Use correct timestamp format for the source
- Use correct incremental field names
- Include all logging blocks
- Include all error handling

### Step 5: Verify Quality Gates
Before delivering, I will verify:
✅ New load config matches template for source
✅ Datasource config updated correctly
✅ Workflow(s) updated with proper structure
✅ Timestamp format correct for source
✅ Incremental field handling correct
✅ All logging blocks present
✅ All error handling blocks present

---

## Source-Specific Considerations

### Google BigQuery
- Use `inc_field` (NOT `incremental_field`)
- Use SQL Server timestamp format
- Add to appropriate datasource list (BigQuery or inc)

### Klaviyo
- Use `.000000` timestamp format (6 decimals, NO Z)
- Check if dual field names needed (like campaigns)
- Add to `inc_datasources` or `hist_datasources` list

### OneTrust
- Use `.000Z` timestamp format (3 decimals, WITH Z)
- Consider monthly batch processing for historical
- Add to appropriate datasource list

### Shopify v2
- Use ISO 8601 timestamp format
- Historical uses `created_at`, incremental uses `updated_at`
- Add to appropriate datasource list

---

## Example Output

For adding `orders` object to `shopify_v2`:

### Files Created/Updated:
1. ✅ Created: `ingestion/config/shopify_v2_orders_load.yml`
2. ✅ Updated: `ingestion/config/shopify_v2_datasources.yml` (added orders to inc_datasources)
3. ✅ Updated: `ingestion/shopify_v2_ingest_inc.dig` (workflow already handles new datasource)

### Verification Complete:
✅ Load config uses ISO 8601 timestamp format
✅ Incremental field set to `updated_at`
✅ Datasource config updated with orders entry
✅ Workflow will automatically process new object
✅ All logging blocks present
✅ Error handling present

---

## After Generation

### 1. Upload Credentials (if new credentials needed)
```bash
cd ingestion
td wf secrets --project ingestion --set @credentials_ingestion.json
```

### 2. Test Syntax
```bash
td wf check {source}_ingest_inc.dig
# or
td wf check {source}_ingest_hist.dig
```

### 3. Run Workflow to Ingest New Object
```bash
td wf run {source}_ingest_inc.dig
# or
td wf run {source}_ingest_hist.dig
```

### 4. Monitor Ingestion
```sql
SELECT * FROM mck_src.ingestion_log
WHERE source_name = '{source}'
  AND table_name = '{source}_{object}'
ORDER BY time DESC
LIMIT 10
```

### 5. Verify Data
```sql
SELECT COUNT(*) as row_count,
       MIN(time) as first_record,
       MAX(time) as last_record
FROM mck_src.{source}_{object}
```

---

## Production-Ready Guarantee

All generated/updated code will:
- ✅ Match existing patterns exactly
- ✅ Use correct timestamp format for source
- ✅ Include all required logging
- ✅ Include all error handling
- ✅ Work seamlessly with existing workflow
- ✅ Be production-ready immediately

---

**Ready to proceed? Provide the required information (source name, object name, table name, incremental field, start date, mode) and I'll generate/update all required files in ONE response using exact templates from documentation.**
