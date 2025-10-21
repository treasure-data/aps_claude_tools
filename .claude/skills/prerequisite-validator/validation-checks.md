# Validation Checks Reference

This document provides detailed validation logic for the prerequisite-validator skill.

## Table of Contents
0. [Client Name Validation](#client-name-validation) **← NEW CRITICAL CHECK**
1. [Database Connectivity Checks](#database-connectivity-checks)
2. [Table Existence Checks](#table-existence-checks)
3. [Schema Validation Checks](#schema-validation-checks)
4. [Credential Checks](#credential-checks)
5. [Naming Convention Checks](#naming-convention-checks)
6. [Data Quality Checks](#data-quality-checks)

---

## Client Name Validation

### Check 0: Client Name & Database Validation

**Purpose**: Ensure user provides REAL client name, not documentation examples like "client_src" or "demo_db"

**Severity**: CRITICAL ❌ (blocks workflow creation)

**When to Run**: ALWAYS FIRST, before any other validation

**Detection Logic**:

**Step 1: Parse Database Names from User Message**
```python
# Look for database patterns in user's message:
patterns = [
    r"(\w+)\.",                    # database.table format
    r"from (\w+)",                 # from database
    r"database:\s*(\w+)",          # database: name
    r"source database:\s*(\w+)",   # source database: name
    r"staging database:\s*(\w+)"   # staging database: name
]

# Extract all database names mentioned
databases_mentioned = extract_all_matches(user_message, patterns)
```

**Step 2: Check Against Generic Name Blocklist**
```python
GENERIC_DATABASES = [
    "client_src",
    "client_stg",
    "demo_db",
    "demo_db_stg",
    "test_db",
    "sample_db",
    "example_db"
]

for db in databases_mentioned:
    if db in GENERIC_DATABASES:
        return CRITICAL_ERROR(
            f"Generic database name detected: {db}"
        )
```

**Step 3: Extract Client Prefix**
```python
# From real database names, extract client:
def extract_client_prefix(database_name):
    """
    acme_src → client = 'acme'
    nike_stg → client = 'nike'
    walmart_staging → client = 'walmart'
    """
    suffixes = ['_src', '_stg', '_staging', '_raw', '_unification']
    for suffix in suffixes:
        if database_name.endswith(suffix):
            return database_name[:-len(suffix)]
    return database_name  # No suffix found
```

**Step 4: Validate Client Name**
```python
def validate_client_name(client):
    """Ensure client name is valid"""
    if not client:
        return False
    if len(client) < 2 or len(client) > 30:
        return False
    if not re.match(r'^[a-zA-Z0-9_]+$', client):
        return False
    if client in GENERIC_DATABASES:
        return False
    return True
```

**Step 5: Verify Databases Exist**

**MCP Tool**: `mcp__demo_treasuredata__list_databases`

```python
# Get all databases from TD
all_databases = mcp__demo_treasuredata__list_databases()

# Check expected databases
client_src = f"{client}_src"
client_stg = f"{client}_stg"

results = {
    "source_db": client_src in all_databases,
    "staging_db": client_stg in all_databases,
    "client_config": "client_config" in all_databases
}
```

**Success Criteria**:
- ✅ Client name provided (not generic)
- ✅ Client name passes validation rules
- ✅ Source database exists OR user confirms creation
- ✅ Staging database exists OR user acknowledges it will be created

**CRITICAL Failure Output**:
```markdown
❌ CRITICAL: Generic Database Name Detected

You mentioned: "client_src"

This is a documentation example name and CANNOT be used for production workflows.

🔧 Action Required:
Please provide your actual client name or project prefix.

Examples:
- Client: "acme" → Databases: acme_src, acme_stg
- Client: "nike" → Databases: nike_src, nike_stg
- Client: "retail_analytics" → Databases: retail_analytics_src, retail_analytics_stg

**Question**: What is your client name or prefix?
_________________________________________________

Once provided, I'll verify these databases exist:
- {client}_src (source data)
- {client}_stg (staging/transformed data)
- client_config (shared configuration)
```

**Success Output**:
```markdown
✅ Client Configuration Validated

**Client Details:**
- Client name: acme
- Source database: acme_src ✅ (exists, 45 tables)
- Staging database: acme_stg ✅ (exists, 23 tables)
- Config database: client_config ✅ (shared, exists)

**Ready to proceed** with workflow generation using these databases.
```

**Warning Output** (database doesn't exist):
```markdown
⚠️ Client Configuration - Database Creation Needed

**Client Details:**
- Client name: acme ✅ (validated)
- Source database: acme_src ❌ (does not exist)
- Staging database: acme_stg ❌ (does not exist)

**Action Required:**
Create these databases before running workflows:

```sql
-- Run in TD console or via API:
CREATE DATABASE acme_src;
CREATE DATABASE acme_stg;
```

**Proceed?** (Y/n): ___
```

**Alternative Flow** (User Doesn't Mention Any Database):
```markdown
ℹ️ Client Configuration Required

Before generating workflows, please provide:

**Option 1: Provide Client Name** (recommended)
Client name/prefix: _______________

This will build databases as:
- Source: {client}_src
- Staging: {client}_stg
- Config: client_config (shared)

**Option 2: Provide Custom Database Names**
If you use different naming conventions:
- Source database: _______________
- Staging database: _______________
- Config database: _______________
```

**Client Name Validation Rules**:
```python
def is_valid_client_name(name):
    """
    Valid: alphanumeric + underscores
    Length: 2-30 characters
    Not in generic blocklist
    """
    rules = {
        "alphanumeric_only": re.match(r'^[a-zA-Z0-9_]+$', name),
        "reasonable_length": 2 <= len(name) <= 30,
        "not_generic": name not in GENERIC_DATABASES,
        "not_empty": bool(name and name.strip())
    }

    failed_rules = [k for k, v in rules.items() if not v]
    return len(failed_rules) == 0, failed_rules
```

**Edge Cases**:

1. **User says "use client_src"**:
   ```
   ❌ BLOCK → Ask for real client name
   ```

2. **User provides table without database prefix**:
   ```
   ℹ️ Ask: "Which database is this table in? Or provide your client name."
   ```

3. **User has custom naming** (e.g., "acme_raw" instead of "acme_src"):
   ```
   ✅ ACCEPT → Use custom names as provided
   Store: source_db = "acme_raw", staging_db = "acme_staging"
   ```

4. **Multiple databases mentioned**:
   ```
   Extract client from first database
   Validate all mentioned databases exist
   Report on each database separately
   ```

5. **User explicitly says "I'm testing with demo_db"**:
   ```
   ⚠️ WARN: "Demo/test databases won't persist to production.
            Use real client names for production workflows."
   Allow to proceed with warning
   ```

**Integration with Workflow Generation**:

After successful validation:
```python
# Store for use in slash commands
validated_config = {
    "client_name": "acme",
    "source_db": "acme_src",
    "staging_db": "acme_stg",
    "client_config": "client_config",
    "databases_exist": True
}

# Pass to agents:
# All generated files will use "acme_src", "acme_stg", etc.
# No generic "client_src" in production files
```

**This check prevents the #1 deployment failure**: Using documentation example names in production workflows.

---

## Database Connectivity Checks

### Check 1: Current Database Context

**Purpose**: Verify MCP connection to Treasure Data is active

**MCP Tool**: `mcp__demo_treasuredata__current_database`

**Success Criteria**:
- Tool returns database name without error
- Database name is not empty

**Failure Actions**:
```
❌ Database connection failed
Action: Check your TD_API_KEY and TD_DATABASE environment variables
Help: Verify MCP server is running: check settings.local.json
```

### Check 2: Available Databases

**Purpose**: List all databases user has access to

**MCP Tool**: `mcp__demo_treasuredata__list_databases`

**Success Criteria**:
- Returns list of databases (can be empty for new accounts)
- No authentication errors

**Use Case**:
- Verify target database exists
- Suggest alternatives if target not found
- Show user what they have access to

---

## Table Existence Checks

### Check 3: Source Table Exists

**Purpose**: Verify each source table mentioned by user exists

**MCP Tool**: `mcp__demo_treasuredata__describe_table`

**Success Criteria**:
- Table description returns successfully
- Column count > 0

**Failure Actions**:
```
❌ Table 'database.table_name' does not exist
Action: Verify table name spelling
Help: List available tables in database:
  Use: mcp__demo_treasuredata__list_tables with database parameter
```

**Pattern Matching**:
```
User input formats to handle:
- "database.table"           → database=database, table=table
- "table"                    → database=current, table=table
- "database.schema.table"    → parse appropriately
- "table_name_histunion"     → detect histunion suffix
```

### Check 4: Table Has Data

**Purpose**: Verify table is not empty (indicates potential upstream issue)

**MCP Tool**: `mcp__demo_treasuredata__query`

**Query**:
```sql
SELECT COUNT(*) as row_count
FROM {database}.{table}
LIMIT 1
```

**Success Criteria**:
- Query executes successfully
- row_count > 0

**Warning Condition**:
```
⚠️ Table '{table}' exists but is empty (0 rows)
This may indicate:
- Ingestion workflow hasn't run yet
- Upstream data source has no data
- Time range filter excluding all data

Recommendation: Verify ingestion workflow completed successfully
```

---

## Schema Validation Checks

### Check 5: Required 'time' Column

**Purpose**: Verify table has TD's required 'time' column for time-based operations

**Method**: Parse output from `describe_table`

**Success Criteria**:
- Column named 'time' exists
- Type is BIGINT or LONG

**Failure Actions**:
```
❌ CRITICAL: Table '{table}' missing required 'time' column
Impact: Cannot perform incremental loads or time-based filtering
Action: Check if this is a valid TD table
Help: All TD tables should have a 'time' column (unix timestamp)
```

### Check 6: Identifier Columns Present

**Purpose**: For unification/staging workflows, verify user identifier columns exist

**Method**: Check column names for common patterns

**Common Identifiers to Look For**:
```
Email identifiers:
- email, email_address, user_email, customer_email, mail

User ID identifiers:
- user_id, customer_id, profile_id, id, external_id

Phone identifiers:
- phone, phone_number, mobile, mobile_number

Cookie/Device identifiers:
- cookie_id, device_id, anonymous_id, visitor_id
```

**Success Criteria**:
- At least one identifier column found

**Warning Condition**:
```
⚠️ No common identifier columns detected in '{table}'
Found columns: {list of column names}
This may be expected for event/transaction tables
Recommendation: Verify this is the correct table for unification
```

### Check 7: JSON Columns Detection

**Purpose**: Identify JSON columns that may need extraction in staging

**Method**: Check column types for 'string' and names containing 'properties', 'attributes', 'data', 'json'

**Info Output**:
```
ℹ️ JSON columns detected in '{table}':
- properties (string) - likely contains nested data
- custom_attributes (string)

Recommendation: Consider extracting commonly-used fields in staging transformation
```

### Check 8: Column Count Sanity Check

**Purpose**: Flag unusually large or small schemas

**Thresholds**:
- **Warning if > 200 columns**: May indicate schema explosion from JSON
- **Warning if < 3 columns**: May indicate incomplete ingestion
- **Info if 50-200 columns**: Normal range

**Output**:
```
⚠️ Table '{table}' has {count} columns (very high)
This may indicate:
- JSON fields were auto-expanded
- Source schema is very wide
- Possible ingestion configuration issue

Recommendation: Review ingestion configuration for JSON handling
```

---

## Credential Checks

### Check 9: Required Secrets by Source Type

**Purpose**: Remind user to set required TD secrets based on data source

**Method**: Parse user intent and source type from conversation

**Secret Requirements by Source**:

```yaml
bigquery:
  - bigquery_service_account
  format: JSON file with service account credentials

klaviyo:
  - klaviyo_api_key
  format: API key starting with 'pk_'

shopify:
  - shopify_api_token
  - shopify_shop_url
  format: Token and shop domain

onetrust:
  - onetrust_client_id
  - onetrust_client_secret
  format: OAuth client credentials

sftp:
  - sftp_username
  - sftp_password or sftp_ssh_key
  format: Username and password or SSH key
```

**Output Format**:
```
⚠️ Credentials Required for {source}

Before workflow execution, set the following secrets:

td secret:set {secret_name} --project {project_name}

Required secrets for {source}:
✓ {secret_name_1} - {description}
✓ {secret_name_2} - {description}

To verify secrets are set:
td secret:list --project {project_name}
```

**Note**: This is a reminder only - skill cannot verify if secrets are actually set (TD limitation)

---

## Naming Convention Checks

### Check 10: Histunion Table Naming

**Purpose**: Verify tables follow naming convention for histunion outputs

**Pattern**: `{source}_{object}_histunion`

**Examples**:
- ✅ `klaviyo_events_histunion`
- ✅ `shopify_orders_histunion`
- ⚠️ `klaviyo_events` - missing _histunion suffix
- ⚠️ `events_histunion` - missing source prefix

**Warning Output**:
```
⚠️ Table name '{table}' doesn't follow histunion convention

Expected pattern: {source}_{object}_histunion
Example: klaviyo_events_histunion

Current name: {table}
This is acceptable but may cause confusion in pipeline tracking
```

### Check 11: Staging Table Naming

**Purpose**: Verify staging tables follow naming convention

**Pattern**: `{source}_{object}`

**Expected in**: Database names ending in `` or containing `staging`

**Warning Output**:
```
ℹ️ Target table naming

For staging tables, recommended pattern:
- Table: {source}_{object}
- Database: {client}_stg

Current: {database}.{table}
```

### Check 12: Database Naming Convention

**Purpose**: Check database names follow environment patterns

**Common Patterns**:
```
Source/Raw layer:
- {client}_src, {client}_raw, {client}_ingestion

Histunion layer:
- {client}_histunion, {client}_hist

Staging layer:
- {client}, {client}_stage

Unification layer:
- {client}_unification, {client}_unified
```

**Info Output**:
```
ℹ️ Database layer detected: {layer}

Current database: {database_name}
Detected layer: {source|histunion|staging|unification|unknown}
```

---

## Data Quality Checks

### Check 13: Recent Data Freshness

**Purpose**: Verify table has recent data (not stale)

**MCP Query**:
```sql
SELECT
  MAX(time) as latest_timestamp,
  FROM_UNIXTIME(MAX(time)) as latest_datetime,
  COUNT(*) as total_rows
FROM {database}.{table}
```

**Success Criteria**:
- Latest timestamp within last 7 days (configurable)

**Warning Condition**:
```
⚠️ Data freshness warning for '{table}'

Latest record: {days} days ago ({datetime})
Total records: {count}

This may indicate:
- Ingestion workflow not running on schedule
- Source system has stopped generating data
- Date filter issue

Recommendation: Check ingestion workflow schedule and last run status
```

### Check 14: Duplicate Check (Sampling)

**Purpose**: Quick check for obvious duplication issues

**MCP Query**:
```sql
SELECT
  COUNT(*) as total_records,
  COUNT(DISTINCT time) as distinct_timestamps
FROM {database}.{table}
LIMIT 10000
```

**Warning Condition**:
```
ℹ️ Duplication check (sample)

Total records: {total}
Distinct timestamps: {distinct}
Ratio: {ratio}

If ratio is very low (< 0.1), may indicate duplicate ingestion
This is informational only - full deduplication happens in staging
```

---

## Validation Severity Levels

### CRITICAL (Blocks workflow) ❌
- Database connection fails
- Source table does not exist
- Table has 0 columns
- Missing required 'time' column

**Action**: Do not proceed with workflow creation. User must fix.

### WARNING (Proceed with caution) ⚠️
- Table is empty
- Unusual column count
- Naming convention mismatch
- Data not fresh (> 7 days old)
- Credentials reminder

**Action**: Show warnings clearly. User can proceed but should investigate.

### INFO (FYI only) ℹ️
- JSON columns detected
- Database layer detection
- Naming convention suggestions
- Identifier columns found

**Action**: Provide helpful context. No action required.

---

## Validation Check Execution Order

Run checks in this order for best UX:

1. **Database connectivity** (fail fast if no connection)
2. **Table existence** (no point checking schema if table doesn't exist)
3. **Schema validation** (required columns, types)
4. **Data quality** (freshness, row counts)
5. **Naming conventions** (informational)
6. **Credentials reminder** (end with action items)

---

## Performance Guidelines

- **Parallel validation**: Check multiple tables concurrently
- **Smart sampling**: Use LIMIT for data quality checks
- **Caching**: Store describe_table results for session
- **Fail fast**: Stop on critical errors, don't waste time on remaining checks
- **Timeout**: Set reasonable timeouts for MCP queries (10s recommended)

---

## Example Validation Sequences

### Sequence 1: Staging Transformation
```
User: "Transform klaviyo_events_histunion to staging"

Checks to run:
1. ✅ Database connectivity
2. ✅ Table exists (client_src.klaviyo_events_histunion)
3. ✅ Has 'time' column
4. ✅ Has identifier columns (email, profile_id)
5. ℹ️ JSON columns detected (properties)
6. ✅ Data freshness (updated 2 hours ago)
7. ✅ Target database exists (client_stg)
8. ℹ️ Naming conventions OK

Result: READY → Suggest /cdp-staging:transform-table
```

### Sequence 2: ID Unification
```
User: "Run ID unification on customer_profiles and order_events"

Checks to run:
1. ✅ Database connectivity
2. ✅ Both tables exist
3. ✅ Identifier columns check
   - customer_profiles: email, customer_id, phone ✅
   - order_events: email, customer_id ✅
4. ℹ️ Common identifiers: email, customer_id
5. ⚠️ phone only in profiles (expected)
6. ✅ Data freshness

Result: READY → Suggest /cdp-unification:unify-setup
```

### Sequence 3: New Ingestion
```
User: "Create Shopify ingestion workflow"

Checks to run:
1. ✅ Database connectivity
2. ✅ Target database exists (shopify_src)
3. ⚠️ Credentials reminder (shopify_api_token, shopify_shop_url)
4. ℹ️ No source tables yet (expected for new ingestion)

Result: READY with WARNING → Remind about credentials → Suggest /cdp-ingestion:ingest-new
```

---

**This reference guide ensures consistent, thorough validation across all CDP workflow types.**
