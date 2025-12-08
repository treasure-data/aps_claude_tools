# Template: Adding New Ingestion Source

## Overview
This document provides a step-by-step template for adding a new data source to the ingestion project. Follow these exact steps to ensure production-ready, reproducible code.

---

## Prerequisites Checklist

Before starting, gather the following information:

- [ ] **Connector Type**: What Treasure Data connector will you use? (e.g., `klaviyo`, `shopify_v2`, `salesforce`)
- [ ] **API Documentation**: Link to connector documentation
- [ ] **Authentication Method**: OAuth, API key, username/password?
- [ ] **Data Objects**: What objects/tables will you ingest? (e.g., contacts, orders, events)
- [ ] **Incremental Support**: Does the connector support incremental loading?
- [ ] **Incremental Fields**: What field names for incremental (both table and API)?
- [ ] **Timestamp Format**: What timestamp format does the connector expect?
- [ ] **Rate Limits**: What are the API rate limits?
- [ ] **Credentials**: Do you have test/production credentials?

---

## Step-by-Step Process

### Step 1: Choose Workflow Template

Review existing sources and choose the most similar template:

| Choose This Template | If Your Source... |
|---------------------|-------------------|
| **Google BigQuery** | Imports from data warehouse, supports unlimited parallel, uses SQL queries |
| **Klaviyo** | Has dual incremental fields (table ≠ API), needs fallback logic, uses 6-decimal timestamps |
| **OneTrust** | Needs both historical backfill and incremental, processes in monthly batches |
| **Shopify v2** | Standard API source, different fields for historical vs. incremental, uses ISO 8601 |

**Decision**: I will use the **[CHOSEN_TEMPLATE]** template because ________________.

---

### Step 2: Determine Critical Settings

#### 2.1 Timestamp Format

Check connector documentation for timestamp format requirements.

**Format Decision**:
- [ ] Klaviyo format (`.000000` - 6 decimals, no Z)
- [ ] OneTrust format (`.000Z` - 3 decimals, with Z)
- [ ] ISO 8601 format (standard with Z)
- [ ] SQL Server format (`yyyy-MM-dd HH:mm:ss.SSS`)
- [ ] Custom format: _______________

**Timestamp Functions** (fill in based on format):

Start time (get max from table):
```sql
[PASTE_EXACT_FUNCTION_FROM_TIMESTAMP-FORMATS.MD]
```

End time (current timestamp):
```sql
[PASTE_EXACT_FUNCTION_FROM_TIMESTAMP-FORMATS.MD]
```

#### 2.2 Incremental Fields

Does your connector support incremental loading?

**If YES**:
- Table field name: ______________ (field in Treasure Data table)
- API field name: ______________ (field the connector expects)
- Are they the same? [ ] Yes [ ] No

**If NO**:
- Set both `incremental_field` and `incremental_field_connector` to empty string
- Use full load pattern

#### 2.3 Parallel Processing

Based on API rate limits:

- [ ] **Unlimited** (`_parallel: true`) - for data warehouses
- [ ] **Limit: 3** (`_parallel: {limit: 3}`) - standard for most APIs
- [ ] **Limit: 1** (`_parallel: {limit: 1}`) - very restrictive APIs
- [ ] **Sequential** (no `_parallel`) - strict ordering required

**Decision**: `_parallel: ______________`

#### 2.4 Mode

- [ ] **Append** - for incremental data (most common)
- [ ] **Replace** - for full snapshots

**Decision**: `mode: ______________`

---

### Step 3: Create Files

#### 3.1 Create Datasources Configuration

**File**: `config/[source_name]_datasources.yml`

```yaml
# Copy and customize from chosen template

hist_datasources:
  - name: [api_object_name]
    table_name: [source]_[object]_hist
    config_file: [source]_[object]_load.yml
    incremental_field: [field_in_table]
    incremental_field_connector: [field_in_api]
    default_start_time: "[TIMESTAMP_IN_CORRECT_FORMAT]"
    workflow_name: [source]_ingest.dig
    mode: [append_or_replace]

inc_datasources:
  - name: [api_object_name]
    table_name: [source]_[object]
    config_file: [source]_[object]_load.yml
    incremental_field: [field_in_table]
    incremental_field_connector: [field_in_api]
    default_start_time: "[TIMESTAMP_IN_CORRECT_FORMAT]"
    workflow_name: [source]_ingest.dig
    mode: [append_or_replace]
```

**Checklist**:
- [ ] Timestamp format matches connector requirements
- [ ] Table and API field names verified from documentation
- [ ] default_start_time is realistic (not in future)
- [ ] Mode is appropriate (append for incremental)

#### 3.2 Create Workflow Files

**File**: `[source_name]_ingest_inc.dig`

```bash
# Copy exact template from chosen source
cp google_bigquery_ingest.dig [source_name]_ingest_inc.dig
# OR
cp klaviyo_ingest_inc.dig [source_name]_ingest_inc.dig
# OR
cp onetrust_ingest.dig [source_name]_ingest.dig
# OR
cp shopify_v2_ingest_inc.dig [source_name]_ingest_inc.dig
```

**Customizations Required**:
1. Update `_export` to include your datasources config
2. Update `source_name` in all logging tasks
3. Update timestamp functions to match your format
4. Update parallel processing setting
5. Verify incremental field handling matches your needs

**File**: `[source_name]_ingest_hist.dig` (if needed)

Repeat above for historical workflow.

#### 3.3 Create Load Configuration Files

For each data object, create:

**File**: `config/[source]_[object]_load.yml`

**Template** (customize based on connector):
```yaml
in:
  type: [connector_type]
  td_authentication_id: [auth_id]

  # Connector-specific parameters
  [data_source|target|object]: ${datasource.name}

  # Incremental parameters (if supported)
  incremental: true
  start_time: ${td.last_results.start_time}
  end_time: ${td.last_results.end_time}
  incremental_field: ${datasource.incremental_field_connector}  # Use connector field!

  # Additional connector-specific params
  [param_name]: [param_value]

filters:
- type: add_time
  to_column:
    name: time
    type: timestamp
  from_value:
    mode: upload_time

out:
  mode: ${datasource.mode}
```

**Check connector documentation for**:
- Exact parameter names
- Required vs. optional parameters
- Special configuration needs

---

### Step 4: Configure Authentication

#### 4.1 Create TD Authentication

In Treasure Data console:
1. Go to Integrations > Authentications
2. Create new authentication for your connector
3. Note the authentication ID

#### 4.2 Add to Credentials Template

Edit `credentials_ingestion.json`:

```json
{
  "[source]_api_key": "your_api_key",
  "[source]_api_secret": "your_api_secret",
  "[source]_base_url": "https://api.source.com",
  "[source]_account_id": "your_account_id"
}
```

**DO NOT COMMIT THIS FILE!**

#### 4.3 Upload Secrets

```bash
td wf secrets --project ingestion --set @credentials_ingestion.json
```

---

### Step 5: Create Source Documentation

**File**: `docs/sources/[source-name].md`

Use this template structure:

```markdown
# [Source Name] Ingestion - Exact Templates

## Overview
[Brief description of what this source does]

## Source Characteristics

| Characteristic | Value |
|---------------|-------|
| **Connector Type** | `[type]` |
| **Parallel Processing** | [setting] |
| **Timestamp Format** | [format] |
| **Incremental Fields** | [description] |
| **Success Logging** | [which template] |
| **Data Sources** | [list objects] |

## Workflow File(s)

[Include EXACT templates - copy from working workflow]

## Datasources Configuration

[Include EXACT template - copy from working config]

## Load Configuration Files

[Include EXACT templates - copy from working configs]

## Critical [Source] Patterns

[List any source-specific quirks or requirements]

## Authentication

[Document auth setup]

## Adding New [Source] Data Objects

[Step-by-step for expanding this source]

## Common Issues

[Troubleshooting guide]

## Critical Reminders

[Bullet list of must-remember items]

## Reference

[Links to related docs and connector documentation]
```

---

### Step 6: Test

#### 6.1 Syntax Test

```bash
# Check workflow syntax
td wf check [source_name]_ingest_inc.dig
```

#### 6.2 Dry Run Test (if supported)

```bash
# Test without actually ingesting
td wf run [source_name]_ingest_inc.dig --dry-run
```

#### 6.3 Small Data Test

**Modify datasources config temporarily**:
```yaml
default_start_time: "[VERY_RECENT_DATE]"  # Just last hour or day
```

**Run workflow**:
```bash
td wf run [source_name]_ingest_inc.dig
```

**Verify**:
- [ ] Workflow completes successfully
- [ ] Data appears in target table
- [ ] ingestion_log shows SUCCESS
- [ ] Record count makes sense
- [ ] Timestamp fields populated correctly

#### 6.4 Incremental Test

1. Run workflow first time
2. Note record count
3. Wait (or manually update source data)
4. Run workflow again
5. Verify only new/updated records ingested

---

### Step 7: Deploy

#### 7.1 Reset to Production Settings

Restore `default_start_time` to actual production start date.

#### 7.2 Push to Production

```bash
td wf push ingestion
```

#### 7.3 Set up Schedule (if needed)

Edit workflow file:
```yaml
# Uncomment and customize:
# timezone: America/New_York
# schedule:
#   cron>: 0 0 * * *  # Daily at midnight
```

---

### Step 8: Document and Update

#### 8.1 Update CLAUDE.md

Add your source to the "Source-Specific Documentation" section:

```markdown
- **[Your Source]**: `docs/sources/[source-name].md`
  - [Brief description]
  - [Key feature 1]
  - [Key feature 2]
```

#### 8.2 Update This Template

If you discovered any new patterns or gotchas, add them to this template for future sources.

---

## Decision Tree for Template Selection

```
What type of source are you adding?

├─ Data Warehouse (BigQuery, Redshift, Snowflake)
│   └─ Use: Google BigQuery template
│       - Unlimited parallel
│       - Query-based incremental
│       - SQL Server timestamp format
│
├─ Marketing/CRM API with complex incremental
│   └─ Use: Klaviyo template
│       - Dual incremental fields
│       - 6-decimal timestamps
│       - Fallback logic
│
├─ Privacy/Compliance API needing historical backfill
│   └─ Use: OneTrust template
│       - Dual-mode workflow
│       - Monthly batch processing
│       - Skip logic
│
└─ Standard REST API with simple incremental
    └─ Use: Shopify v2 template
        - Standard incremental pattern
        - ISO 8601 timestamps
        - Separate hist/inc workflows
```

---

## Verification Checklist

Before considering the source "complete":

### Files Created
- [ ] `config/database.yml`
- [ ] `config/[source]_datasources.yml`
- [ ] `[source]_ingest_inc.dig` (and `_hist.dig` if needed)
- [ ] `config/[source]_[object]_load.yml` (for each object)
- [ ] `sql/log_ingestion_.*.yml` (sql logging files)
- [ ] `docs/sources/[source-name].md`
- [ ] Updated `credentials_ingestion.json` (not committed!)

### Configuration Verified
- [ ] Timestamp format matches connector requirements
- [ ] Incremental fields (table and API) are correct
- [ ] Parallel processing appropriate for rate limits
- [ ] Mode (append/replace) appropriate for use case
- [ ] Authentication ID is correct
- [ ] Secrets uploaded to TD Workflows

### Testing Completed
- [ ] Syntax check passed
- [ ] Small data test successful
- [ ] Incremental test successful
- [ ] Data quality verified
- [ ] Logging working correctly

### Documentation Updated
- [ ] Source documentation created
- [ ] CLAUDE.md updated
- [ ] Any new patterns documented

---

## Common Mistakes to Avoid

1. **Wrong timestamp format** - Most common error. ALWAYS verify from connector docs.
2. **Mixing incremental fields** - Use table field in SQL, connector field in YAML.
3. **Forgetting to check for null** - Always check `${datasource.incremental_field != "" && ...}`
4. **Using wrong logging template** - BigQuery uses `log_ingestion_success_2.sql`, others use `log_ingestion_success.sql`
5. **Not uploading secrets** - Workflow will fail if secrets not in TD.
6. **Committing credentials** - NEVER commit `credentials_ingestion.json`!
7. **Wrong parallel setting** - Unlimited only for data warehouses, limit for APIs.
8. **Skipping tests** - Always test with small data first.
9. **Not documenting quirks** - Future you will thank present you.
10. **Copying without understanding** - Know WHY each part of template exists.

---

## Getting Help

If stuck:

1. **Check existing source docs**: `docs/sources/[similar-source].md`
2. **Check pattern docs**: `docs/patterns/[pattern-name].md`
3. **Check connector docs**: TD documentation for your connector
4. **Check ingestion_log**: Errors often have helpful messages
5. **Ask team**: Someone may have done something similar

---

## Success Criteria

Your new source is successful when:

- ✅ Workflow runs without errors
- ✅ Data arrives in target table
- ✅ Incremental loading works correctly (if applicable)
- ✅ Logging captures start, success, and errors
- ✅ Documentation is complete and accurate
- ✅ Another team member can replicate your work from docs
- ✅ Code is production-ready and maintainable

---

## Example: Adding Pinterest Connector

See actual implementation:
- Workflow: `pinterest_ingest.dig`
- Configs: `config/pinterest_campaigns_load.yml`, `config/pinterest_ad_groups_load.yml`
- Pattern used: Similar to Shopify v2 (standard API incremental)

---

## Template Maintenance

**When to update this template**:
- New connector type added with unique patterns
- Common mistake discovered
- Better testing approach found
- New TD feature that changes best practices

**Who maintains**: Team lead or most experienced engineer

**Review frequency**: Quarterly or when adding 3+ new sources

---

## Quick Reference Links

- **Workflow Patterns**: `docs/patterns/workflow-patterns.md`
- **Timestamp Formats**: `docs/patterns/timestamp-formats.md`
- **Logging Patterns**: `docs/patterns/logging-patterns.md`
- **Incremental Patterns**: `docs/patterns/incremental-patterns.md`
- **Google BigQuery Example**: `docs/sources/google-bigquery.md`
- **Klaviyo Example**: `docs/sources/klaviyo.md`
- **OneTrust Example**: `docs/sources/onetrust.md`
- **Shopify v2 Example**: `docs/sources/shopify-v2.md`
