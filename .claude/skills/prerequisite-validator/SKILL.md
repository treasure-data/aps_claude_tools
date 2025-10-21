---
name: prerequisite-validator
description: Automatically validates Treasure Data environment and prerequisites before CDP workflow creation. Checks database connectivity, table existence, schema validation, and credential setup to prevent workflow failures.
---

# Prerequisite Validator Skill

## Purpose

Proactively validate the environment and prerequisites before users create CDP workflows. This skill runs automatically when users express intent to create ingestion, staging, unification, or hist-union workflows, catching issues early and reducing failed deployments.

## When to Activate

Automatically trigger when user mentions or requests:
- Creating new ingestion workflows
- Adding data sources or objects
- Setting up staging transformations
- Running ID unification
- Creating hist-union workflows
- Any CDP pipeline setup or deployment
- Troubleshooting workflow failures

**Keywords to watch for:**
- "ingest", "ingestion", "load data", "import"
- "transform", "staging", "clean data"
- "unify", "unification", "merge identities"
- "hist-union", "combine historical"
- "create workflow", "setup pipeline"

## Validation Workflow

### Step 0: Client Name & Database Validation (NEW - CRITICAL)

**ALWAYS run this check FIRST before any workflow creation:**

```
1. Detect database names mentioned by user:
   - Parse user's message for database references
   - Identify: source database, staging database, config database
   - Action: Extract client prefix from database names

2. Check for GENERIC database names (CRITICAL):
   - ❌ REJECT: "client_src", "client_stg", "demo_db", "demo_db_stg"
   - ❌ REJECT: Any database matching documentation examples
   - ✅ ACCEPT: Real client names (e.g., "acme_src", "nike_stg")

3. If GENERIC names detected:
   - STOP validation immediately
   - Prompt user for REAL client name:
     ```
     ⚠️ GENERIC DATABASE NAME DETECTED

     You mentioned: "{detected_database}"

     This appears to be a documentation example name. For production workflows,
     please provide your actual client name/prefix.

     Question: What is your client name or prefix?
     Examples: "acme", "nike", "walmart", "retail_co"

     This will build your databases as:
     - Source: {client}_src
     - Staging: {client}_stg
     - Config: config_db (shared)
     ```
   - Wait for user response
   - Validate provided client name (alphanumeric, underscores only)
   - Build database names: {client}_src, {client}_stg

4. Verify databases exist using MCP:
   - Use: mcp__demo_treasuredata__list_databases
   - Check if {client}_src exists
   - Check if {client}_stg exists (or inform it will be created)
   - Action: Report which databases exist vs need creation

5. Store client name for workflow generation:
   - Pass to slash command context
   - Ensure all generated files use real client name
```

### Step 1: Database Connectivity Check

Use MCP tools to verify TD connection:

```
1. Check current database context:
   - Use: mcp__demo_treasuredata__current_database
   - Verify: Connection successful
   - Action: If fails, inform user to check TD credentials

2. List available databases:
   - Use: mcp__demo_treasuredata__list_databases
   - Verify: User has access to target databases
   - Action: Show available databases if target not found
```

### Step 2: Source Table Validation

When user mentions specific tables to process:

```
1. Verify source tables exist:
   - Use: mcp__demo_treasuredata__describe_table
   - For each table user mentions
   - Action: Report which tables exist vs missing

2. Validate table schemas:
   - Check column count > 0
   - Verify 'time' column exists (required for TD)
   - Check for expected identifier columns (email, user_id, etc.)
   - Action: Warn if schema looks incomplete or suspicious

3. Check table data availability:
   - Use: mcp__demo_treasuredata__query with LIMIT 1
   - Verify: Table has data (not empty)
   - Action: Warn if table is empty (may indicate upstream issue)
```

### Step 3: Target Database Validation

```
1. Verify target database exists:
   - Check if database in user's list
   - Action: Suggest creation command if missing

2. Check write permissions (when possible):
   - Attempt to query existing tables in target DB
   - Action: Warn if permissions might be insufficient
```

### Step 4: Credential Validation

For ingestion workflows specifically:

```
1. Identify required credentials from source type:
   - BigQuery: Service account JSON
   - Klaviyo: API key
   - Shopify: API token, shop URL
   - OneTrust: Client ID, client secret
   - SFTP: SSH key or password

2. Check if user has mentioned credentials:
   - Parse user's message for credential references
   - Action: Remind user to set TD secrets if not mentioned

3. Provide secret setup guidance:
   - Show exact td secret:set command
   - Reference credential naming conventions
```

### Step 5: Schema Compatibility Check

For staging and unification workflows:

```
1. Check source-target compatibility:
   - Verify source table schema matches expected pattern
   - Check for required columns based on workflow type
   - Action: Warn about potential schema mismatches

2. Validate naming conventions:
   - Check table names follow pattern: {source}_{object}_histunion
   - Verify staging tables end in _staging
   - Action: Suggest corrections if naming is inconsistent
```

## Output Format

Provide clear, structured validation report:

```markdown
## 🔍 Prerequisite Validation Report

### ✅ Client Configuration
- Client name: acme
- Source database: acme_src (exists ✅)
- Staging database: acme_stg (exists ✅)
- Config database: config_db (exists ✅)

### ✅ Database Connectivity
- Connection: PASS
- Current database: {database_name}
- Available databases: {count} found

### ✅ Source Tables
- acme_src.klaviyo_events_histunion: PASS (234 columns, 1.2M rows)
- acme_src.klaviyo_profiles_histunion: PASS (156 columns, 450K rows)
- acme_src.shopify_orders_histunion: WARNING - Table empty

### ✅ Target Database
- acme_stg: PASS (exists, accessible)

### ⚠️ Credentials
- klaviyo_api_key: NOT VERIFIED (reminder to set)

  Set credential before workflow execution:
  ```bash
  td secret:set klaviyo_api_key --project {project_name}
  ```

### ✅ Schema Validation
- Required 'time' column: PASS (all tables)
- Email columns detected: 2 tables
- User ID columns detected: 3 tables

---

**Overall Status**: READY with 1 warning

**Recommendation**: Set klaviyo_api_key credential, then proceed with workflow creation.

**Next Step**: You can now use `/cdp-staging:transform-batch` to transform these tables.
```

## Validation Rules Reference

### Critical Issues (BLOCK workflow creation)
- ❌ Generic database name detected (client_src, demo_db, etc.)
- ❌ Database connection fails
- ❌ Source table does not exist
- ❌ Source table has 0 columns (corrupted)
- ❌ Missing required 'time' column

### Warnings (WARN but allow to proceed)
- ⚠️ Source table is empty (0 rows)
- ⚠️ Credentials not verified
- ⚠️ Naming convention doesn't match pattern
- ⚠️ Target database not found (user might create it)

### Info (FYI only)
- ℹ️ Table has many columns (>100)
- ℹ️ Multiple databases available
- ℹ️ Schema includes JSON columns

## Handoff to Slash Commands

After validation completes:

1. **If CRITICAL issues found**:
   - DO NOT suggest slash commands
   - Provide fix instructions
   - Wait for user to resolve

2. **If WARNINGS only**:
   - Show warnings clearly
   - Suggest slash command to proceed
   - Note what needs attention

3. **If ALL PASS**:
   - Confirm ready state
   - Recommend appropriate slash command based on intent
   - Pre-fill parameters from validation (table names, databases)

## Example Usage Flow

**User says:** "I need to transform klaviyo_events_histunion and klaviyo_profiles_histunion to staging"

**Skill activates:**
1. Detects intent: staging transformation
2. Validates database connection ✅
3. Checks both tables exist ✅
4. Verifies schemas ✅
5. Checks target database ✅
6. Reports validation success
7. Suggests: `/cdp-staging:transform-batch` with pre-filled table names

**User can proceed** with confidence that environment is ready.

## Integration with Existing Agents

This skill **complements** (not replaces) existing agents:

- **Before agent runs**: Skill validates environment
- **Agent runs**: Focused on file generation (current behavior)
- **After agent runs**: Other skills can validate output

**No changes needed** to existing slash commands or agents. This skill adds a safety layer.

## Edge Cases to Handle

1. **User provides database.table format**: Parse correctly
2. **User mentions tables from multiple databases**: Validate all
3. **User doesn't specify database**: Use current_database from MCP
4. **Table names have special characters**: Handle quotes properly
5. **Large number of tables**: Batch validation, summarize results

## Performance Considerations

- **Fast validation**: Use MCP describe_table (metadata only)
- **Smart sampling**: Query LIMIT 1 to check data existence
- **Parallel checks**: Validate multiple tables concurrently where possible
- **Caching**: Remember validation results within same conversation

## Error Handling

If validation checks fail:
- Catch MCP errors gracefully
- Provide helpful error messages
- Suggest next steps
- Don't crash or stop the conversation

## Success Metrics

Track improvement in:
- Reduced workflow failures due to missing tables
- Fewer credential-related errors
- Faster time-to-deployment
- User confidence in proceeding

---

**Remember**: This skill is about being helpful and proactive, not blocking users. Validate thoroughly, report clearly, and guide users toward success.
