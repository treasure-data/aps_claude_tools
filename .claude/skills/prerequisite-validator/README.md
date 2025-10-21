# Prerequisite Validator Skill

**Status**: ✅ Ready for Use
**Version**: 1.0.0
**Created**: October 2025

## Overview

The `prerequisite-validator` skill automatically validates your Treasure Data environment before CDP workflow creation, catching issues early and preventing deployment failures.

## What This Skill Does

### Automatic Activation
This skill activates automatically when you mention:
- Creating ingestion workflows
- Transforming tables to staging
- Running ID unification
- Setting up hist-union workflows
- Any CDP pipeline setup

### Validations Performed

1. **Database Connectivity** ✅
   - Verifies TD connection is active
   - Checks current database context
   - Lists available databases

2. **Table Validation** ✅
   - Confirms source tables exist
   - Verifies tables have data (not empty)
   - Checks schema completeness

3. **Schema Checks** ✅
   - Required 'time' column present
   - Identifier columns detected (email, user_id, etc.)
   - JSON columns identified for extraction

4. **Data Quality** ✅
   - Data freshness (last updated timestamp)
   - Row count sanity checks
   - Column count validation

5. **Naming Conventions** ℹ️
   - Histunion naming patterns
   - Staging naming patterns
   - Database layer detection

6. **Credential Reminders** ⚠️
   - Required secrets by source type
   - Setup commands provided
   - Naming convention guidance

## Files in This Skill

```
prerequisite-validator/
├── SKILL.md                 # Main skill definition (Claude reads this)
├── validation-checks.md     # Detailed validation logic reference
├── mcp-queries.md          # MCP query examples and patterns
└── README.md               # This file (for humans)
```

## How to Use

### For Users
You don't need to do anything special! Just use Claude Code normally:

**Example 1:**
```
You: "I want to transform klaviyo_events_histunion to staging"

Claude: [Skill activates automatically]
🔍 Running prerequisite validation...

✅ Database connectivity: PASS
✅ Source table exists: client_src.klaviyo_events_histunion (234 columns)
✅ Data freshness: Updated 2 hours ago
ℹ️ JSON columns detected: properties

Ready to proceed! Use: /cdp-staging:transform-table
```

**Example 2:**
```
You: "Create a new Shopify ingestion workflow"

Claude: [Skill activates automatically]
🔍 Prerequisite check for Shopify ingestion...

✅ Database connectivity: PASS
✅ Target database exists: shopify_src
⚠️ Credentials required:
   - shopify_api_token
   - shopify_shop_url

Set these before workflow execution:
td secret:set shopify_api_token --project ingestion_main

Ready to proceed with setup!
```

### For Developers

The skill uses MCP tools to query Treasure Data:
- `mcp__demo_treasuredata__current_database`
- `mcp__demo_treasuredata__list_databases`
- `mcp__demo_treasuredata__describe_table`
- `mcp__demo_treasuredata__query`

See `mcp-queries.md` for detailed query examples.

## Integration with Existing Workflows

### This Skill Complements (Not Replaces)

**Before:**
```
User → Slash Command → Agent → Generate Files → Manual Validation
```

**Now:**
```
User intent
    ↓
[prerequisite-validator] ← Validates environment
    ↓
User → Slash Command → Agent → Generate Files
    ↓
User deploys with confidence
```

**Key Point:** Your existing slash commands and agents work exactly as before. This skill adds a validation layer BEFORE workflow creation.

## Validation Severity Levels

### ❌ CRITICAL (Blocks Workflow)
- Database connection fails
- Source table does not exist
- Table has 0 columns
- Missing required 'time' column

**Action:** User must fix before proceeding

### ⚠️ WARNING (Proceed with Caution)
- Table is empty
- Unusual column count
- Naming convention mismatch
- Data not fresh (> 7 days old)
- Credentials reminder

**Action:** User can proceed but should investigate

### ℹ️ INFO (FYI Only)
- JSON columns detected
- Database layer detection
- Naming convention suggestions
- Identifier columns found

**Action:** Helpful context, no action required

## Example Validation Reports

### Success Case
```
## 🔍 Prerequisite Validation Report

### ✅ Database Connectivity
- Connection: PASS
- Current database: client_src

### ✅ Source Tables
- client_src.klaviyo_events_histunion: PASS (234 columns, 1.2M rows)
- client_src.klaviyo_profiles_histunion: PASS (156 columns, 450K rows)

### ✅ Target Database
- client_staging: PASS

### ✅ Schema Validation
- Required 'time' column: PASS
- Email columns: Found in 2 tables
- User ID columns: Found in 2 tables

Overall Status: ✅ READY

Next Step: /cdp-staging:transform-batch
```

### Warning Case
```
## 🔍 Prerequisite Validation Report

### ✅ Database Connectivity
- Connection: PASS

### ⚠️ Source Tables
- client_src.orders_histunion: WARNING - Table empty (0 rows)

Possible causes:
- Ingestion workflow hasn't run yet
- Source system has no data
- Check ingestion workflow status

### ⚠️ Credentials
- shopify_api_token: NOT VERIFIED

Set credential:
td secret:set shopify_api_token --project ingestion_main

Overall Status: ⚠️ READY with warnings

Recommendation: Investigate empty table and set credentials before proceeding.
```

### Failure Case
```
## 🔍 Prerequisite Validation Report

### ❌ Database Connectivity
- Connection: FAIL

Error: Unable to connect to Treasure Data
Check your TD_API_KEY environment variable

### ❌ Cannot Proceed
Fix database connection before creating workflows.

Help:
1. Verify TD_API_KEY is set
2. Check MCP server is running
3. Review .claude/settings.local.json
```

## Performance Characteristics

- **Fast**: Most validations complete in < 5 seconds
- **Parallel**: Checks multiple tables concurrently
- **Smart Sampling**: Uses LIMIT for data quality checks
- **Cached**: Results cached within conversation session

## Testing the Skill

### Test 1: Valid Table
```
You: "Transform client_src.klaviyo_events_histunion"
Expected: ✅ All checks pass
```

### Test 2: Missing Table
```
You: "Transform nonexistent_table"
Expected: ❌ Table does not exist
```

### Test 3: Empty Table
```
You: "Transform empty_table"
Expected: ⚠️ Table exists but is empty
```

### Test 4: New Ingestion
```
You: "Create Shopify ingestion"
Expected: ✅ Pass with credentials reminder
```

## Troubleshooting

### Skill Not Activating

**Symptom:** Claude doesn't run validation automatically

**Possible Causes:**
1. Skill description doesn't match user intent keywords
2. SKILL.md file not in correct location
3. Malformed YAML frontmatter

**Solution:**
```bash
# Verify skill file exists
ls -la .claude/skills/prerequisite-validator/SKILL.md

# Check frontmatter format
head -10 .claude/skills/prerequisite-validator/SKILL.md
```

### MCP Query Failures

**Symptom:** "Cannot connect to database" errors

**Solution:**
1. Check TD_API_KEY environment variable
2. Verify MCP server is running
3. Test MCP connection manually:
   ```
   You: "List my TD databases"
   ```

### False Positives

**Symptom:** Skill reports errors but table actually exists

**Solution:**
1. Check table name spelling (case-sensitive)
2. Verify database context
3. Use full table name: `database.table`

## Future Enhancements

Potential additions for v2.0:
- [ ] Workflow session status checking
- [ ] Historical failure pattern detection
- [ ] Automatic retry suggestions
- [ ] Performance benchmarking
- [ ] Cost estimation
- [ ] Schema drift detection

## Support

For issues or questions:
1. Review validation-checks.md for detailed logic
2. Check mcp-queries.md for query examples
3. Test MCP connectivity manually
4. Verify skill file structure

## Metrics to Track

Monitor these to measure skill impact:
- Reduction in failed workflow runs
- Time saved in pre-deployment checks
- User confidence in proceeding
- Fewer credential-related errors
- Faster issue identification

## Credits

**Designed for:** APS CDP Tools Marketplace
**Integrated with:** CDP Ingestion, Staging, Unification, Hist-Union plugins
**Uses:** Treasure Data MCP Server

---

**Version History:**

- **v1.0.0** (Oct 2024) - Initial release
  - Database connectivity checks
  - Table existence validation
  - Schema validation
  - Data quality checks
  - Credential reminders
  - MCP query integration
