# CDP Ingestion Plugin

**Production-ready data ingestion automation for Treasure Data**

---

## Overview

The CDP Ingestion plugin automates the creation of data ingestion workflows for Treasure Data, supporting multiple source systems with template-driven generation. It handles both historical (full-load) and incremental ingestion patterns with built-in error handling, logging, and credential management.

### Purpose

Accelerate data ingestion setup from hours to minutes by automating:
- Digdag workflow creation
- Data source configuration
- Load configuration with column mappings
- Credential management via TD secrets
- Error handling and retry logic
- Incremental loading strategies

---

## Supported Data Sources

### Currently Supported (6+ sources)

1. **BigQuery** - Google Cloud data warehouse
   - Historical and incremental modes
   - Support for partitioned tables
   - Column-level data type mapping

2. **Klaviyo** - Marketing automation platform
   - 10+ standard objects (events, profiles, campaigns, lists, metrics, etc.)
   - API-based ingestion
   - Full and incremental sync

3. **Shopify** - E-commerce platform
   - Products, customers, orders, inventory
   - REST API integration
   - Incremental updates via updated_at timestamps

4. **OneTrust** - Privacy and consent management
   - User profiles and consent records
   - GDPR compliance data
   - Incremental sync

5. **Pinterest** - Social media advertising
   - Ad campaign data
   - Conversion tracking
   - Analytics data

6. **SFTP** - Secure file transfer
   - CSV, JSON, Parquet file support
   - Pattern-based file selection
   - Archive after ingestion

---

## Features

### Template-Driven Generation
- **Exact Templates**: Production-tested patterns with zero improvisation
- **Batch Generation**: All files created in single response
- **Consistency**: Same structure across all implementations

### Comprehensive Configuration
- **Data Source Config**: Connection details, authentication, rate limits
- **Load Config**: Table mappings, column definitions, data types
- **Workflow Logic**: Scheduling, dependencies, error handling

### Error Handling
- **Retry Logic**: Automatic retries with exponential backoff
- **Error Notifications**: Integration with alerting systems
- **Data Validation**: Schema validation, null handling

### Credential Management
- **TD Secrets**: Secure credential storage
- **No Hardcoded Values**: All sensitive data via secrets
- **Credential Rotation**: Support for credential updates

### Incremental Loading
- **Watermark Management**: Automatic tracking of last loaded timestamp
- **Deduplication**: Handling of duplicate records
- **Backfill Support**: Historical load capabilities

---

## Slash Commands

### `/cdp-ingestion:ingest-new`

Create a new data source ingestion workflow from scratch.

**Usage:**
```bash
/cdp-ingestion:ingest-new
```

**Prompts for:**
- Source type (BigQuery, Shopify, SFTP, etc.)
- Source name (e.g., "shopify_prod")
- Target database
- Objects/tables to ingest
- Credential details
- Schedule configuration

**Generates:**
- `ingestion/{source}_ingest_inc.dig` - Main workflow
- `datasource/{source}.yml` - Source configuration
- `load/{source}_{object}.yml` - Load configs per object

---

### `/cdp-ingestion:ingest-add-klaviyo`

Specialized command for complete Klaviyo ingestion setup with all standard objects.

**Usage:**
```bash
/cdp-ingestion:ingest-add-klaviyo
```

**Prompts for:**
- Klaviyo API key (stored as TD secret)
- Target database
- Schedule (daily, hourly, etc.)

**Automatically includes:**
- klaviyo_events
- klaviyo_profiles
- klaviyo_campaigns
- klaviyo_lists
- klaviyo_metrics
- klaviyo_flows
- klaviyo_templates
- klaviyo_segments
- klaviyo_forms
- klaviyo_coupons

**Generates:**
- Complete workflow with all objects
- Proper dependencies between objects
- Incremental loading for events
- Full reload for reference data

---

### `/cdp-ingestion:ingest-add-object`

Add a new object/table to an existing data source workflow.

**Usage:**
```bash
/cdp-ingestion:ingest-add-object
```

**Prompts for:**
- Existing source name
- New object/table name
- Column definitions
- Load strategy (incremental vs full)

**Generates:**
- Updated workflow file
- New load configuration
- Updated dependencies

**Use Cases:**
- Adding new Shopify products table to existing workflow
- Extending BigQuery ingestion with new dataset
- Adding custom Klaviyo metric

---

### `/cdp-ingestion:ingest-validate-wf`

Validate generated workflow and configuration files against quality gates.

**Usage:**
```bash
/cdp-ingestion:ingest-validate-wf
```

**Validates:**
- YAML syntax correctness
- Required fields presence
- Credential references
- Table naming conventions
- Schedule format
- Error handling completeness
- Logging presence

**Output:**
```
✓ Workflow syntax valid
✓ All required fields present
✓ Credentials properly referenced
✓ Error handling configured
✓ Logging enabled
⚠ Warning: Schedule frequency < 1 hour
```

---

## File Structure

### Generated Files

```
ingestion/
├── bigquery_ingest_inc.dig          # BigQuery workflow
├── klaviyo_ingest_inc.dig           # Klaviyo workflow
└── shopify_ingest_inc.dig           # Shopify workflow

datasource/
├── bigquery.yml                     # BigQuery connection config
├── klaviyo.yml                      # Klaviyo API config
└── shopify.yml                      # Shopify API config

load/
├── bigquery_analytics_users.yml     # Load config per table
├── klaviyo_events.yml
├── klaviyo_profiles.yml
├── shopify_products.yml
└── shopify_orders.yml
```

### Workflow File Structure (`.dig`)

```yaml
timezone: UTC

schedule:
  daily>: 02:00:00

_export:
  td:
    database: ${td.database}

+ingest_klaviyo_events:
  td_load>: load/klaviyo_events.yml
  database: ${td.database}
  table: klaviyo_events

  _error:
    echo>: "Ingestion failed for klaviyo_events"
    td_run>: error_notification_workflow
```

### Data Source Config (`.yml`)

```yaml
in:
  type: klaviyo
  api_key: ${secret:klaviyo_api_key}
  object: events
  incremental: true
  incremental_column: updated_at
  start_date: 2024-01-01

config:
  retry_limit: 3
  retry_interval: 300
  rate_limit: 100
```

### Load Config (`.yml`)

```yaml
in:
  type: klaviyo

out:
  mode: append

columns:
  - {name: event_id, type: string}
  - {name: event_name, type: string}
  - {name: profile_id, type: string}
  - {name: timestamp, type: long}
  - {name: properties, type: json}
```

---

## Usage Examples

### Example 1: Ingest Shopify Data

```bash
/cdp-ingestion:ingest-new
```

**Inputs:**
- Source type: Shopify
- Source name: shopify_production
- Target database: ecommerce_raw
- Objects: products, customers, orders, inventory
- API credentials: (prompted for shop URL, API key)
- Schedule: Daily at 2 AM UTC

**Generated:**
- `ingestion/shopify_production_ingest_inc.dig`
- `datasource/shopify_production.yml`
- `load/shopify_production_products.yml`
- `load/shopify_production_customers.yml`
- `load/shopify_production_orders.yml`
- `load/shopify_production_inventory.yml`

**Result:** Complete Shopify ingestion ready to deploy

---

### Example 2: Add Klaviyo Marketing Data

```bash
/cdp-ingestion:ingest-add-klaviyo
```

**Inputs:**
- API Key: pk_abc123... (stored as secret)
- Database: marketing_raw
- Schedule: Hourly

**Generated:**
- Complete workflow with 10+ Klaviyo objects
- All load configurations
- Proper incremental loading for events
- Full reload for lists and campaigns

**Result:** Full Klaviyo data pipeline in minutes

---

### Example 3: Extend Existing BigQuery Ingestion

```bash
/cdp-ingestion:ingest-add-object
```

**Inputs:**
- Existing source: bigquery_analytics
- New object: user_sessions
- Columns: session_id, user_id, start_time, end_time, page_views
- Load strategy: Incremental (partition by date)

**Generated:**
- Updated workflow file
- New load config for user_sessions
- Maintains existing objects

---

## Workflow Deployment

### Step 1: Generate Files

```bash
/cdp-ingestion:ingest-new
# Follow prompts
```

### Step 2: Validate

```bash
/cdp-ingestion:ingest-validate-wf
```

### Step 3: Deploy to TD

```bash
# Using TD CLI
td wf push ingestion_project

# Or using API
curl -X POST https://api.treasuredata.com/v3/projects \
  -H "Authorization: TD1 $TD_API_KEY" \
  -d @workflow_bundle.tar.gz
```

### Step 4: Configure Secrets

```bash
# Store API keys securely
td secret:set klaviyo_api_key

# Verify secret
td secret:list
```

### Step 5: Test Run

```bash
# Manual execution
td wf start ingestion_project klaviyo_ingest_inc

# Monitor
td wf sessions --project ingestion_project
```

---

## Best Practices

### 1. Use Incremental Loading
- Always prefer incremental over full load when supported
- Define proper watermark columns (updated_at, created_at)
- Handle late-arriving data

### 2. Proper Scheduling
- Avoid peak hours for heavy loads
- Consider API rate limits
- Add buffer time between dependent workflows

### 3. Error Handling
- Configure retry logic for transient failures
- Set up alerting for persistent errors
- Log all ingestion metrics

### 4. Credential Security
- Never hardcode credentials
- Use TD secrets for all sensitive data
- Rotate credentials regularly
- Audit secret access

### 5. Data Validation
- Validate schema on ingestion
- Check for null values in critical fields
- Monitor row counts and data quality
- Set up data quality alerts

### 6. Performance Optimization
- Use parallel ingestion for independent objects
- Configure appropriate batch sizes
- Optimize API call patterns
- Monitor ingestion duration

---

## Common Patterns

### Pattern 1: Full + Incremental

```yaml
# Historical load (one-time)
+load_historical:
  td_load>: load/shopify_orders_hist.yml
  database: ecommerce_raw
  table: shopify_orders_hist

# Daily incremental
+load_incremental:
  td_load>: load/shopify_orders_inc.yml
  database: ecommerce_raw
  table: shopify_orders
  require>: [load_historical]
```

### Pattern 2: Multiple Sources with Dependencies

```yaml
# Load source data first
+load_shopify:
  td_load>: load/shopify_orders.yml

+load_klaviyo:
  td_load>: load/klaviyo_events.yml

# Then trigger transformation
+trigger_staging:
  require>: [load_shopify, load_klaviyo]
  td_run>: staging_transformation
```

### Pattern 3: Error Notification

```yaml
+ingest_data:
  td_load>: load/source.yml

  _error:
    http>: https://hooks.slack.com/services/YOUR/WEBHOOK
    method: POST
    content:
      text: "Ingestion failed: ${session_id}"
```

---

## Troubleshooting

### Issue: API Rate Limit Exceeded

**Solution:**
- Reduce batch size in datasource config
- Increase retry_interval
- Add delays between requests
```yaml
config:
  batch_size: 100  # Reduce from default
  retry_interval: 600  # Increase to 10 minutes
```

### Issue: Authentication Failure

**Solution:**
- Verify secret is set: `td secret:list`
- Check API key format and permissions
- Ensure secret reference matches: `${secret:key_name}`

### Issue: Schema Mismatch

**Solution:**
- Update load config with correct column definitions
- Check source API documentation
- Use `type: json` for complex nested objects

### Issue: Duplicate Records

**Solution:**
- Verify incremental column is correct
- Add deduplication in load config:
```yaml
out:
  mode: append
  unique_columns: [id, updated_at]
```

---

## Quality Gates

All generated workflows must pass:

1. **Syntax Validation** - Valid YAML, no parse errors
2. **Required Fields** - All mandatory fields present
3. **Credential Security** - No hardcoded secrets
4. **Error Handling** - `_error` blocks configured
5. **Logging** - Proper echo/logging statements
6. **Naming Conventions** - Follow TD standards
7. **Schedule Format** - Valid cron/schedule syntax
8. **Dependencies** - Proper `require>` usage

---

## Template Reference

### Supported Sources

| Source | Type | Incremental | Authentication |
|--------|------|-------------|----------------|
| BigQuery | Database | Yes | Service Account |
| Klaviyo | API | Yes | API Key |
| Shopify | API | Yes | API Token |
| OneTrust | API | Yes | Client Credentials |
| Pinterest | API | Yes | OAuth Token |
| SFTP | File | Partial | SSH Key/Password |

---

## Advanced Features

### Custom Transformations

Add inline transformations in load config:

```yaml
columns:
  - {name: email, type: string, transformer: "lower(email)"}
  - {name: created_at, type: long, format: "%Y-%m-%d %H:%M:%S"}
```

### Conditional Loading

Load based on conditions:

```yaml
filters:
  - column: status
    operator: equals
    value: active
  - column: created_at
    operator: greater_than
    value: ${last_load_time}
```

### Parallel Processing

Load multiple objects in parallel:

```yaml
+ingest_parallel:
  _parallel: true

  +load_products:
    td_load>: load/products.yml

  +load_customers:
    td_load>: load/customers.yml
```

---

## Support

For issues, questions, or feature requests:
- Review generated files in `ingestion/`, `datasource/`, `load/` directories
- Check validation output from `ingest-validate-wf`
- Consult TD documentation: https://docs.treasuredata.com

---

**Version:** 1.0.0
**Last Updated:** 2024-10-10
**Maintained by:** APS CDP Team
