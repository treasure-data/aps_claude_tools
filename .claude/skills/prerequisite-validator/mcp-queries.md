# MCP Query Examples for Prerequisite Validation

This document contains ready-to-use MCP queries for the prerequisite-validator skill.

## Table of Contents
1. [Database Connectivity Queries](#database-connectivity-queries)
2. [Table Validation Queries](#table-validation-queries)
3. [Schema Analysis Queries](#schema-analysis-queries)
4. [Data Quality Queries](#data-quality-queries)
5. [Identifier Discovery Queries](#identifier-discovery-queries)

---

## Database Connectivity Queries

### Query 1: Get Current Database
```
MCP Tool: mcp__demo_treasuredata__current_database
Parameters: none

Expected Response:
{
  "database": "client_src"
}

Use Case: Verify connection and get default database context
```

### Query 2: List All Databases
```
MCP Tool: mcp__demo_treasuredata__list_databases
Parameters: none

Expected Response:
{
  "databases": [
    "client_src",
    "client_staging",
    "client_unification",
    "sample_datasets"
  ]
}

Use Case: Verify target database exists, suggest alternatives
```

---

## Table Validation Queries

### Query 3: Check Table Exists and Get Schema
```
MCP Tool: mcp__demo_treasuredata__describe_table
Parameters:
  - database: "client_src" (optional if using current)
  - table: "klaviyo_events_histunion"

Expected Response:
{
  "database": "client_src",
  "table": "klaviyo_events_histunion",
  "columns": [
    {"name": "time", "type": "bigint"},
    {"name": "event_id", "type": "varchar"},
    {"name": "profile_id", "type": "varchar"},
    {"name": "email", "type": "varchar"},
    {"name": "properties", "type": "varchar"}
  ]
}

Use Case: Verify table exists and get full schema
```

### Query 4: List Tables in Database
```
MCP Tool: mcp__demo_treasuredata__list_tables
Parameters:
  - database: "client_src"

Expected Response:
{
  "tables": [
    "klaviyo_events_histunion",
    "klaviyo_profiles_histunion",
    "shopify_orders_histunion"
  ]
}

Use Case: Discover available tables when user doesn't specify exact names
```

---

## Schema Analysis Queries

### Query 5: Quick Row Count Check
```
MCP Tool: mcp__demo_treasuredata__query
Parameters:
  - sql: "SELECT COUNT(*) as row_count FROM client_src.klaviyo_events_histunion"
  - limit: 1

Expected Response:
{
  "columns": ["row_count"],
  "data": [[1234567]]
}

Use Case: Verify table has data
```

### Query 6: Check for Required 'time' Column
```
MCP Tool: mcp__demo_treasuredata__describe_table
Parameters:
  - table: "klaviyo_events_histunion"

Then parse columns array:
  - Filter: column["name"] == "time"
  - Verify: column["type"] in ["bigint", "long"]

Use Case: Validate TD required column exists
```

### Query 7: Detect Identifier Columns
```
MCP Tool: mcp__demo_treasuredata__describe_table
Parameters:
  - table: "customer_profiles"

Parse columns for common identifier patterns:

email_patterns = ["email", "email_address", "user_email", "customer_email", "mail"]
id_patterns = ["user_id", "customer_id", "profile_id", "id", "external_id"]
phone_patterns = ["phone", "phone_number", "mobile", "mobile_number"]

Match column names (case-insensitive):
  - Email: "email" → FOUND
  - User ID: "customer_id" → FOUND
  - Phone: "phone_number" → FOUND

Use Case: Identify available identifiers for unification
```

### Query 8: Detect JSON Columns
```
MCP Tool: mcp__demo_treasuredata__describe_table
Parameters:
  - table: "klaviyo_events_histunion"

Parse columns:
  - Type: "varchar" or "string"
  - Name contains: "properties", "attributes", "data", "json", "metadata"

Example matches:
  - "properties" (varchar) → Likely JSON
  - "custom_attributes" (varchar) → Likely JSON
  - "event_data" (varchar) → Likely JSON

Use Case: Identify columns that may need JSON extraction in staging
```

---

## Data Quality Queries

### Query 9: Data Freshness Check
```
MCP Tool: mcp__demo_treasuredata__query
Parameters:
  - sql: |
      SELECT
        MAX(time) as latest_timestamp,
        MIN(time) as earliest_timestamp,
        COUNT(*) as total_rows,
        COUNT(DISTINCT time) as distinct_timestamps
      FROM client_src.klaviyo_events_histunion
  - limit: 1

Expected Response:
{
  "columns": ["latest_timestamp", "earliest_timestamp", "total_rows", "distinct_timestamps"],
  "data": [[1729580400, 1672531200, 1234567, 892345]]
}

Then calculate:
  - Latest date: FROM_UNIXTIME(latest_timestamp)
  - Days since latest: (NOW - latest_timestamp) / 86400
  - Warn if > 7 days

Use Case: Check if data is fresh and actively updating
```

### Query 10: Sample Records Check
```
MCP Tool: mcp__demo_treasuredata__query
Parameters:
  - sql: "SELECT * FROM client_src.klaviyo_events_histunion"
  - limit: 5

Expected Response:
{
  "columns": ["time", "event_id", "profile_id", "email", "properties"],
  "data": [
    [1729580400, "evt_123", "prof_456", "user@example.com", "{...}"],
    [1729580401, "evt_124", "prof_457", "user2@example.com", "{...}"],
    ...
  ]
}

Use Case: Quick sanity check that data looks reasonable
```

### Query 11: Column Null Check
```
MCP Tool: mcp__demo_treasuredata__query
Parameters:
  - sql: |
      SELECT
        COUNT(*) as total_rows,
        COUNT(email) as email_not_null,
        COUNT(user_id) as user_id_not_null,
        COUNT(phone) as phone_not_null
      FROM client_src.customer_profiles_histunion
      LIMIT 1000
  - limit: 1

Expected Response:
{
  "columns": ["total_rows", "email_not_null", "user_id_not_null", "phone_not_null"],
  "data": [[1000, 950, 1000, 450]]
}

Calculate null percentages:
  - email: 95% populated (5% null)
  - user_id: 100% populated (0% null)
  - phone: 45% populated (55% null)

Use Case: Assess identifier column completeness for unification
```

### Query 12: Table Size Estimation
```
MCP Tool: mcp__demo_treasuredata__query
Parameters:
  - sql: |
      SELECT
        COUNT(*) as estimated_rows,
        COUNT(DISTINCT TD_TIME_FORMAT(time, 'yyyy-MM-dd')) as distinct_days
      FROM client_src.klaviyo_events_histunion
      WHERE time > TD_TIME_PARSE('2024-01-01', 'yyyy-MM-dd')
  - limit: 1

Expected Response:
{
  "columns": ["estimated_rows", "distinct_days"],
  "data": [[5234567, 298]]
}

Use Case: Estimate table size for performance planning
```

---

## Identifier Discovery Queries

### Query 13: Email Domain Analysis
```
MCP Tool: mcp__demo_treasuredata__query
Parameters:
  - sql: |
      SELECT
        SPLIT_PART(email, '@', 2) as email_domain,
        COUNT(*) as count
      FROM client_src.customer_profiles_histunion
      WHERE email IS NOT NULL
      GROUP BY SPLIT_PART(email, '@', 2)
      ORDER BY count DESC
  - limit: 10

Expected Response:
{
  "columns": ["email_domain", "count"],
  "data": [
    ["gmail.com", 45678],
    ["yahoo.com", 23456],
    ["example.com", 12345],
    ...
  ]
}

Use Case: Detect test emails, validate email quality
```

### Query 14: Identifier Overlap Analysis
```
MCP Tool: mcp__demo_treasuredata__query
Parameters:
  - sql: |
      SELECT
        COUNT(DISTINCT email) as unique_emails,
        COUNT(DISTINCT user_id) as unique_user_ids,
        COUNT(DISTINCT phone) as unique_phones,
        COUNT(*) as total_records
      FROM client_src.customer_profiles_histunion
  - limit: 1

Expected Response:
{
  "columns": ["unique_emails", "unique_user_ids", "unique_phones", "total_records"],
  "data": [[89234, 95678, 45123, 100000]]
}

Analysis:
  - Email coverage: 89.2%
  - User ID coverage: 95.7% (best identifier)
  - Phone coverage: 45.1%

Use Case: Determine best identifiers for unification
```

### Query 15: Cross-Table Identifier Match
```
MCP Tool: mcp__demo_treasuredata__query
Parameters:
  - sql: |
      SELECT
        COUNT(DISTINCT p.email) as profiles_with_email,
        COUNT(DISTINCT e.email) as events_with_email,
        COUNT(DISTINCT p.user_id) as profiles_with_id,
        COUNT(DISTINCT e.user_id) as events_with_id
      FROM client_src.customer_profiles_histunion p
      FULL OUTER JOIN client_src.customer_events_histunion e
        ON p.email = e.email OR p.user_id = e.user_id
  - limit: 1

Use Case: Assess identifier overlap between tables for unification
```

---

## Workflow-Specific Query Sets

### For Staging Transformations

**Required Queries:**
1. Query 3: Describe source table (get schema)
2. Query 5: Row count check (verify data exists)
3. Query 8: JSON column detection
4. Query 9: Data freshness check

**Optional Queries:**
5. Query 10: Sample records
6. Query 11: Null checks for key columns

### For ID Unification

**Required Queries:**
1. Query 7: Detect identifier columns in each table
2. Query 14: Identifier uniqueness analysis
3. Query 11: Null percentage for identifiers

**Optional Queries:**
4. Query 13: Email domain analysis (quality check)
5. Query 15: Cross-table identifier overlap

### For New Ingestion Workflows

**Required Queries:**
1. Query 1: Current database
2. Query 2: List databases (verify target exists)
3. Query 4: List existing tables (check for conflicts)

**No source table queries needed** (tables don't exist yet)

### For Hist-Union Workflows

**Required Queries:**
1. Query 3: Describe historical table
2. Query 3: Describe incremental table
3. Query 9: Data freshness (both tables)
4. Schema compatibility check (compare columns from both describes)

---

## Query Performance Tips

### 1. Use LIMIT Appropriately
```sql
-- Good: Fast estimation
SELECT COUNT(*) FROM table LIMIT 1000

-- Bad: Full table scan
SELECT COUNT(*) FROM billion_row_table
```

### 2. Use Time-Based Filtering
```sql
-- Good: Only recent data for freshness check
SELECT MAX(time)
FROM table
WHERE time > TD_TIME_PARSE('2024-01-01')

-- Bad: Full table scan
SELECT MAX(time) FROM table
```

### 3. Leverage TD Functions
```sql
-- Good: Use TD's optimized functions
WHERE time > TD_TIME_PARSE('2024-10-01', 'yyyy-MM-dd')

-- Bad: String comparison
WHERE TD_TIME_FORMAT(time, 'yyyy-MM-dd') > '2024-10-01'
```

### 4. Sample Before Analyzing
```sql
-- Good: Sample first
SELECT * FROM large_table
WHERE TD_TIME_RANGE(time, '2024-10-20', '2024-10-21')
LIMIT 100

-- Bad: Analyze everything
SELECT * FROM large_table
```

---

## Error Handling Examples

### Handle Missing Table
```python
try:
  result = mcp__demo_treasuredata__describe_table(table="nonexistent")
except Error as e:
  if "not found" in str(e):
    return "❌ Table does not exist"
  else:
    return f"❌ Error: {e}"
```

### Handle Query Timeout
```python
try:
  result = mcp__demo_treasuredata__query(
    sql="SELECT COUNT(*) FROM huge_table",
    timeout=10000  # 10 seconds
  )
except TimeoutError:
  return "⚠️ Query timed out - table may be very large"
```

### Handle Permission Errors
```python
try:
  result = mcp__demo_treasuredata__list_databases()
except PermissionError:
  return "❌ Insufficient permissions - check API key"
```

---

## Query Result Parsing Patterns

### Pattern 1: Extract Column Names
```python
describe_result = mcp__demo_treasuredata__describe_table(table="users")
column_names = [col["name"] for col in describe_result["columns"]]
column_types = {col["name"]: col["type"] for col in describe_result["columns"]}

# Check for required column
has_time = "time" in column_names
time_type = column_types.get("time", None)
```

### Pattern 2: Parse Count Queries
```python
query_result = mcp__demo_treasuredata__query(
  sql="SELECT COUNT(*) as cnt FROM table"
)

row_count = query_result["data"][0][0]  # First row, first column
is_empty = row_count == 0
```

### Pattern 3: Parse Multi-Column Results
```python
query_result = mcp__demo_treasuredata__query(
  sql="SELECT MAX(time) as latest, COUNT(*) as total FROM table"
)

columns = query_result["columns"]  # ["latest", "total"]
data = query_result["data"][0]      # [1729580400, 1234567]

latest_timestamp = data[columns.index("latest")]
total_rows = data[columns.index("total")]
```

---

## Complete Validation Query Workflow

### Step-by-Step Query Execution

```python
# Step 1: Check database connectivity
current_db = mcp__demo_treasuredata__current_database()
# Result: {"database": "client_src"}

# Step 2: Parse table name from user input
table_full_name = "client_src.klaviyo_events_histunion"
database, table = parse_table_name(table_full_name, current_db)

# Step 3: Verify table exists and get schema
table_schema = mcp__demo_treasuredata__describe_table(
  database=database,
  table=table
)
# Result: {"columns": [...]}

# Step 4: Validate required columns
column_names = [col["name"] for col in table_schema["columns"]]
has_time = "time" in column_names  # ✅ Required
identifier_columns = find_identifier_columns(column_names)  # ✅ Found: email, user_id

# Step 5: Check data freshness
freshness_query = f"""
  SELECT
    MAX(time) as latest,
    COUNT(*) as rows
  FROM {database}.{table}
  LIMIT 1
"""
freshness = mcp__demo_treasuredata__query(sql=freshness_query, limit=1)
latest_time = freshness["data"][0][0]
row_count = freshness["data"][0][1]

# Step 6: Generate validation report
report = generate_validation_report({
  "database": database,
  "table": table,
  "schema": table_schema,
  "row_count": row_count,
  "latest_time": latest_time,
  "identifiers": identifier_columns
})

# Step 7: Return to user
print(report)
```

---

**Use these query examples as a reference when implementing the prerequisite-validator skill.**
