# User Prompt Guide - Getting Expected Outputs

## Overview
This guide provides **exact prompts** users should give to Claude Code to get expected outputs based on the documented templates. These prompts are designed to work with the modular documentation structure.

---

## Table of Contents
- [Replicating Existing Workflows](#replicating-existing-workflows)
- [Adding New Data Sources](#adding-new-data-sources)
- [Modifying Existing Workflows](#modifying-existing-workflows)
- [Debugging and Troubleshooting](#debugging-and-troubleshooting)
- [Documentation Updates](#documentation-updates)
- [Configuration Management](#configuration-management)

---

## Replicating Existing Workflows

### Pattern 1: Replicate Entire Source Integration

**Use Case**: You want to create a workflow similar to an existing source

**Expected Prompt Format**:
```
Create a [NEW_SOURCE] ingestion workflow based on the [EXISTING_SOURCE] pattern.

Requirements:
- Use the same workflow structure as [EXISTING_SOURCE]
- Include [list of data objects/tables]
- Support [incremental/historical/both] modes
- Use [authentication method]
```

**Example Prompts**:

#### Example 1: Replicate Klaviyo for Mailchimp
```
Create a Mailchimp ingestion workflow based on the Klaviyo pattern.

Requirements:
- Use the same workflow structure as Klaviyo
- Include subscribers, campaigns, and email_activity tables
- Support both incremental and historical modes
- Use API key authentication
```

**Expected Output**:
- `mailchimp_ingest_inc.dig` (workflow file)
- `mailchimp_ingest_hist.dig` (workflow file)
- `config/mailchimp_datasources.yml` (datasource config)
- `config/mailchimp_subscribers_load.yml` (table config)
- `config/mailchimp_campaigns_load.yml` (table config)
- `config/mailchimp_email_activity_load.yml` (table config)
- Updated `credentials_ingestion.json` template
- Documentation updates in CLAUDE.md

#### Example 2: Replicate BigQuery for Another Data Warehouse
```
Create a Snowflake ingestion workflow based on the Google BigQuery pattern.

Requirements:
- Use the same workflow structure as BigQuery
- Include separate historical and incremental workflows
- Support monthly batch processing for historical
- Include sales_orders, transactions, and customer_profiles tables
- Use OAuth authentication
```

**Expected Output**:
- `snowflake_ingest_hist.dig`
- `snowflake_ingest_inc.dig`
- `config/snowflake_datasources.yml` with hist_datasources and inc_datasources
- Individual load configs for each table
- Monthly batch processing with skip logic
- Centralized auth in database.yml

---

### Pattern 2: Replicate Single Workflow Type

**Use Case**: You only need incremental OR historical, not both

**Expected Prompt Format**:
```
Create a [NEW_SOURCE] incremental-only ingestion workflow based on the [EXISTING_SOURCE] incremental pattern.

Include:
- [list of tables/objects]
- Incremental field: [field_name]
- Default start time: [timestamp]
```

**Example Prompt**:
```
Create a HubSpot incremental-only ingestion workflow based on the Shopify v2 incremental pattern.

Include:
- contacts table with incremental field: lastmodifieddate
- deals table with incremental field: hs_lastmodifieddate
- companies table with incremental field: hs_lastmodifieddate
- Default start time: 2025-01-01T00:00:00.000Z
```

**Expected Output**:
- `hubspot_ingest_inc.dig` only (no historical)
- `config/hubspot_datasources.yml` with inc_datasources only
- Three load config files
- Parallel processing with limit: 3
- ISO 8601 timestamp format (like Shopify v2)

---

## Adding New Data Sources

### Pattern 3: Add New Source with Decision Support

**Use Case**: You're not sure which template to use

**Expected Prompt Format**:
```
I want to add [NEW_SOURCE] ingestion to this project.

Source details:
- Data type: [API/Data Warehouse/File-based]
- Rate limits: [Yes/No - if yes, specify]
- Incremental support: [Yes/No]
- Timestamp format: [format if known]
- Tables/Objects: [list]

Help me choose the right template and create the implementation.
```

**Example Prompt**:
```
I want to add Salesforce ingestion to this project.

Source details:
- Data type: REST API
- Rate limits: Yes, 100 calls per 20 seconds
- Incremental support: Yes, using SystemModstamp field
- Timestamp format: ISO 8601
- Tables/Objects: Account, Contact, Opportunity, Lead

Help me choose the right template and create the implementation.
```

**Expected Output**:
- Claude will recommend template (likely Shopify v2 or Klaviyo)
- Create complete implementation with all files
- Explain choice of template
- Provide testing instructions

---

### Pattern 4: Add New Object to Existing Source

**Use Case**: Add a new table/object to an existing source

**Expected Prompt Format**:
```
Add [NEW_TABLE] to the existing [SOURCE] ingestion workflow.

Table details:
- Incremental field: [field_name]
- Default start time: [timestamp]
- Mode: [append/replace]
- [Any special requirements]
```

**Example Prompts**:

#### Example 1: Add to Klaviyo
```
Add email_flows table to the existing Klaviyo ingestion workflow.

Table details:
- Incremental field: updated_at (table field), updated (API field)
- Default start time: 2025-01-01T00:00:00.000000
- Mode: append
- Should be included in both incremental and historical workflows
```

**Expected Output**:
- Updated `config/klaviyo_datasources.yml` (added to both inc_datasources and hist_datasources)
- New `config/klaviyo_email_flows_load.yml`
- No workflow file changes needed (uses existing workflows)
- Documentation update

#### Example 2: Add to BigQuery
```
Add customer_reviews table to the existing Google BigQuery ingestion workflow.

Table details:
- Dataset: gold_treasure_data
- BigQuery table name: customer_reviews
- Incremental field: review_date
- Default start time: 2023-01-01 00:00:00.000
- Mode: append
- Should be included in both historical and incremental
```

**Expected Output**:
- Updated `config/google_bq_datasources.yml` (both hist and inc)
- New `config/google_bq_customer_reviews.yml` (query type for inc)
- New `config/google_bq_customer_reviews_historical.yml` (table type for hist)
- No workflow changes needed

---

## Modifying Existing Workflows

### Pattern 5: Change Parallel Processing Settings

**Expected Prompt Format**:
```
Update the parallel processing limit for [SOURCE] [workflow_type] from [current_limit] to [new_limit].

Reason: [explanation]
```

**Example Prompt**:
```
Update the parallel processing limit for Klaviyo incremental workflow from 3 to 5.

Reason: We've increased our API rate limits and want to speed up ingestion.
```

**Expected Output**:
- Modified `klaviyo_ingest_inc.dig` with `_parallel: {limit: 5}`
- Explanation of change
- Warning about API rate limits

---

### Pattern 6: Change Timestamp Format

**Expected Prompt Format**:
```
I'm getting timestamp format errors in [SOURCE] workflow. The error is: [error_message]

Help me fix the timestamp format.
```

**Example Prompt**:
```
I'm getting timestamp format errors in the OneTrust workflow. The error is: "Invalid timestamp format: expected .000Z suffix"

Help me fix the timestamp format.
```

**Expected Output**:
- Review of current timestamp format in workflow
- Correction to use exact format from timestamp-formats.md
- Updated workflow file with corrected SQL
- Explanation of why format was wrong

---

### Pattern 7: Add Monthly Batch Processing

**Expected Prompt Format**:
```
Convert [SOURCE] historical workflow to use monthly batch processing with skip logic like the [REFERENCE_SOURCE] pattern.
```

**Example Prompt**:
```
Convert Klaviyo historical workflow to use monthly batch processing with skip logic like the Google BigQuery pattern.
```

**Expected Output**:
- Modified `klaviyo_ingest_hist.dig` with monthly batch loop
- Skip logic based on ingestion_log
- Date-specific source names for logging
- Updated to use `hist_date_ranges.yml`

---

## Debugging and Troubleshooting

### Pattern 8: Debug Authentication Errors

**Expected Prompt Format**:
```
The [SOURCE] workflow is failing with authentication error: [error_message]

Help me troubleshoot this.
```

**Example Prompt**:
```
The Shopify v2 workflow is failing with authentication error: "Invalid API credentials"

Help me troubleshoot this.
```

**Expected Output**:
- Check workflow files for correct secret references
- Verify secret names match credentials_ingestion.json
- Check if td_authentication_id is correct
- Provide verification commands
- Step-by-step fix instructions

---

### Pattern 9: Debug Incremental Processing

**Expected Prompt Format**:
```
The [SOURCE] [table] incremental processing is not working correctly. [Describe issue: no new data, all data reloading, etc.]

Current incremental field: [field_name]
Last run status: [status]
```

**Example Prompt**:
```
The Klaviyo campaigns incremental processing is not working correctly. It's reloading all campaigns every time instead of just new/updated ones.

Current incremental field: updated_at (table), updated (API)
Last run status: SUCCESS, but loads all records
```

**Expected Output**:
- Check incremental field configuration
- Verify incremental_field vs. incremental_field_connector
- Check timestamp format in time query
- Review last_results usage
- Provide SQL to check incremental table
- Fix with exact corrections

---

### Pattern 10: Debug Timestamp Errors

**Expected Prompt Format**:
```
The [SOURCE] workflow is failing with timestamp error: [error_message]

Current timestamp format used: [format]
```

**Example Prompt**:
```
The Google BigQuery workflow is failing with timestamp error: "Invalid datetime format"

Current timestamp format used: ISO 8601 standard
```

**Expected Output**:
- Reference timestamp-formats.md for correct format
- Show exact SQL function needed for BigQuery (SQL Server format)
- Update workflow with correct format
- Explain why specific format is needed

---

## Documentation Updates

### Pattern 11: Update Documentation After Changes

**Expected Prompt Format**:
```
I've made changes to [workflow/config files]. Please update all relevant documentation to reflect these changes.

Changes made:
- [list of changes]
```

**Example Prompt**:
```
I've made changes to the OneTrust workflow and configuration. Please update all relevant documentation to reflect these changes.

Changes made:
- Added consent_receipts table
- Changed parallel limit from 3 to 5
- Updated default_start_time to 2024-01-01
```

**Expected Output**:
- Updated `docs/sources/onetrust.md`
- Updated CLAUDE.md if needed
- Updated README.md if needed
- Version history update

---

### Pattern 12: Create Documentation for New Source

**Expected Prompt Format**:
```
Create complete documentation for the [NEW_SOURCE] integration following the same structure as [EXISTING_SOURCE] documentation.

Include:
- All workflow templates
- All configuration templates
- Timestamp format details
- Common issues section
- Adding new objects guide
```

**Example Prompt**:
```
Create complete documentation for the Salesforce integration following the same structure as Klaviyo documentation.

Include:
- All workflow templates (salesforce_ingest_inc.dig and salesforce_ingest_hist.dig)
- All configuration templates
- Timestamp format details
- Common issues section
- Adding new objects guide
```

**Expected Output**:
- New `docs/sources/salesforce.md` with complete templates
- Updated CLAUDE.md to reference Salesforce
- Updated README.md to include Salesforce in active sources

---

## Configuration Management

### Pattern 13: Add New Authentication ID

**Expected Prompt Format**:
```
Add a new authentication ID for [SOURCE] to the centralized database.yml configuration.

Authentication details:
- Source: [source_name]
- TD Authentication ID: [id_number]
- Environment: [default/specific]
```

**Example Prompt**:
```
Add a new authentication ID for Salesforce to the centralized database.yml configuration.

Authentication details:
- Source: salesforce
- TD Authentication ID: 360500
- Environment: default
```

**Expected Output**:
- Updated `config/database.yml` with new entry:
```yaml
td_authentication_ids:
  salesforce:
    default: 360500
```
- Instructions to update load configs to reference `${td_authentication_ids.salesforce.default}`

---

### Pattern 14: Update Date Ranges for Historical Processing

**Expected Prompt Format**:
```
Extend the historical date ranges in hist_date_ranges.yml to include [time_period].

Current range: [start] to [end]
New range needed: [start] to [end]
```

**Example Prompt**:
```
Extend the historical date ranges in hist_date_ranges.yml to include Q1 and Q2 2024.

Current range: 2023-09-01 to 2023-11-30
New range needed: 2023-09-01 to 2024-06-30
```

**Expected Output**:
- Updated `config/hist_date_ranges.yml` with monthly ranges through June 2024
- Maintains exact format for month_name, start_time, end_time

---

## Best Practices for Prompts

### ✅ DO:

1. **Be Specific About Source**
   - ✅ "Create a Mailchimp ingestion workflow based on Klaviyo"
   - ❌ "Create a new email marketing integration"

2. **Specify Template/Pattern**
   - ✅ "Use the same structure as Google BigQuery historical workflow"
   - ❌ "Create a historical workflow"

3. **Provide Complete Details**
   - ✅ Include table names, field names, timestamps, modes
   - ❌ "Add some tables to Klaviyo"

4. **Reference Existing Patterns**
   - ✅ "With monthly batch processing like OneTrust"
   - ❌ "Make it process in batches"

5. **Be Clear About Scope**
   - ✅ "Update only the incremental workflow"
   - ❌ "Update the Klaviyo workflow"

### ❌ DON'T:

1. **Don't Be Vague**
   - ❌ "Fix the errors in my workflow"
   - ✅ "Fix the timestamp format error in Shopify v2 workflow: [error message]"

2. **Don't Assume Context**
   - ❌ "Add that table we discussed"
   - ✅ "Add customer_reviews table to BigQuery ingestion"

3. **Don't Skip Required Details**
   - ❌ "Add HubSpot integration"
   - ✅ "Add HubSpot with contacts, deals, companies using incremental pattern"

4. **Don't Mix Multiple Unrelated Tasks**
   - ❌ "Add Salesforce, fix Klaviyo, and update all docs"
   - ✅ Split into separate focused prompts

---

## Prompt Templates by Use Case

### Quick Reference

| Use Case | Template Phrase | Expected Output |
|----------|----------------|-----------------|
| New source, similar to existing | "Create [NEW] based on [EXISTING] pattern" | Complete workflow + configs + docs |
| Add table to source | "Add [TABLE] to existing [SOURCE]" | Updated datasources.yml + new load config |
| Fix timestamp | "Fix timestamp format error in [SOURCE]: [ERROR]" | Corrected SQL with exact format |
| Change parallel limit | "Update parallel limit for [SOURCE] from [X] to [Y]" | Modified workflow file |
| Debug auth | "Troubleshoot auth error in [SOURCE]: [ERROR]" | Step-by-step debugging guide + fixes |
| Add monthly batches | "Convert [SOURCE] to monthly batches like [PATTERN]" | Updated workflow with batch logic |
| Update docs | "Update docs for changes to [SOURCE]" | Updated all relevant documentation |
| New auth ID | "Add auth ID for [SOURCE] to database.yml" | Updated centralized auth config |

---

## Examples of Complete Prompt Sequences

### Scenario 1: Adding Complete New Source (Salesforce)

**Prompt 1 (Planning)**:
```
I want to add Salesforce CRM integration to this project.

Salesforce details:
- REST API with rate limits (100 calls per 20 seconds)
- Supports incremental via SystemModstamp field
- ISO 8601 timestamp format
- Need: Account, Contact, Opportunity, Lead tables

Which existing workflow template should I use as a pattern, and why?
```

**Prompt 2 (Implementation)**:
```
Create a complete Salesforce ingestion workflow based on the Shopify v2 pattern.

Requirements:
- Separate incremental and historical workflows
- Include Account, Contact, Opportunity, Lead tables
- Use SystemModstamp as incremental field for all tables
- Default start time: 2024-01-01T00:00:00.000Z
- Parallel limit: 3 (due to API rate limits)
- OAuth authentication with TD auth ID: 360600
```

**Prompt 3 (Documentation)**:
```
Create complete documentation for the Salesforce integration following the exact structure of docs/sources/shopify-v2.md.

Include all templates, timestamp format, common issues, and step-by-step guide for adding new Salesforce objects.
```

**Prompt 4 (Testing)**:
```
Provide a testing checklist and commands for the new Salesforce workflows before deploying to production.
```

---

### Scenario 2: Debugging Existing Workflow

**Prompt 1 (Identify Issue)**:
```
The Klaviyo campaigns incremental workflow is loading all records instead of only new/updated ones.

Current config:
- Incremental field (table): updated_at
- Incremental field (API): updated
- Last run: SUCCESS with 50,000 records (should be ~100)

Help me diagnose the issue.
```

**Prompt 2 (Fix)**:
```
Based on your diagnosis, fix the Klaviyo campaigns configuration to properly use incremental processing.
```

**Prompt 3 (Verify)**:
```
Provide SQL queries to verify that incremental processing is now working correctly for Klaviyo campaigns.
```

---

### Scenario 3: Performance Optimization

**Prompt 1 (Analysis)**:
```
The Google BigQuery historical workflow is taking too long to process.

Current setup:
- Processing 6 tables
- Unlimited parallel for entire workflow
- No batch processing

Analyze and suggest optimizations based on the documented patterns.
```

**Prompt 2 (Implementation)**:
```
Implement monthly batch processing for Google BigQuery historical workflow following the exact pattern documented in docs/sources/google-bigquery.md.

Use the existing hist_date_ranges.yml configuration.
```

**Prompt 3 (Monitoring)**:
```
Add monitoring queries to track the progress of monthly batch processing for BigQuery historical workflow.
```

---

## Validation Prompts

After Claude Code completes a task, use these prompts to validate:

### Validation 1: Completeness Check
```
Verify that all required files have been created/updated for the [SOURCE] integration:
- Workflow files
- Configuration files
- Documentation updates
- Credential template updates

Provide a checklist with ✅ or ❌ for each item.
```

### Validation 2: Pattern Compliance Check
```
Verify that the [SOURCE] implementation follows the exact patterns documented in:
- docs/patterns/workflow-patterns.md
- docs/sources/[reference_source].md

Highlight any deviations from the documented patterns.
```

### Validation 3: Syntax Check
```
Validate the syntax of all workflow and configuration files created for [SOURCE] using td wf check command and report any errors.
```

---

## Common Mistakes to Avoid

### Mistake 1: Not Specifying Which Template
❌ **Bad**: "Create a HubSpot integration"
✅ **Good**: "Create a HubSpot integration based on the Klaviyo pattern"

**Why**: Without specifying a template, Claude might make assumptions that don't match your architecture.

### Mistake 2: Missing Field Details
❌ **Bad**: "Add contacts table to Salesforce"
✅ **Good**: "Add contacts table to Salesforce with incremental field: SystemModstamp, default start: 2024-01-01T00:00:00.000Z"

**Why**: Missing details lead to incorrect default values or assumptions.

### Mistake 3: Vague Error Descriptions
❌ **Bad**: "The workflow isn't working"
✅ **Good**: "The Shopify workflow fails with error: 'Authentication failed - invalid credentials' at step +load_incremental"

**Why**: Specific errors get specific fixes; vague descriptions get generic troubleshooting.

### Mistake 4: Not Referencing Documentation
❌ **Bad**: "Use best practices for timestamps"
✅ **Good**: "Use the timestamp format documented in docs/patterns/timestamp-formats.md for Klaviyo source"

**Why**: Referencing documentation ensures exact patterns are followed.

---

## Summary

The key to getting expected outputs from Claude Code:

1. **Be Specific**: Name exact sources, tables, fields
2. **Reference Templates**: Point to existing patterns to follow
3. **Provide Complete Details**: Include all required parameters
4. **Use Clear Structure**: Follow the prompt templates in this guide
5. **Validate Results**: Use validation prompts after implementation

Remember: The more specific and detailed your prompt, the more accurate and production-ready the output will be.
