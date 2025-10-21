# Snowflake Ingestion - Exact Templates

## Overview
Snowflake ingestion uses the Snowflake connector to import data from Snowflake data warehouse. This source supports both full table imports and incremental loading based on timestamp columns.

## Source Characteristics

| Characteristic | Value |
|---------------|----------|
| **Connector Type** | `snowflake` |
| **Parallel Processing (Incremental)** | Limited (`_parallel: {limit: 10}`) |
| **Parallel Processing (Historical)** | Unlimited (`_parallel: true`) |
| **Timestamp Format** | SQL Server style (`yyyy-MM-dd HH:mm:ss.SSS`) |
| **Incremental Support** | Yes (via query or incremental column) |
| **Success Logging** | `log_ingestion_success.sql` (with time tracking) |
| **Workflows** | Separate hist/inc workflows |

---

## Critical Snowflake Parameters

### Required Parameters
- `type: snowflake` - Connector type
- `td_authentication_id` - TD authentication ID for Snowflake credentials
- `account_name` - Snowflake account identifier (NOT `account`)
- `warehouse` - Snowflake warehouse name
- `schema` - Snowflake schema name
- `db` - Snowflake database name
- `query` - Custom SQL query (Always use this, because table mode is always full load)
- `incremental` - Always set to false

---

## Load Configuration Files

### Type 1: Full Table Load (Non-Incremental)

**File**: `config/snowflake_[table]_load.yml`

**EXACT TEMPLATE**:
```yaml
in:
  type: snowflake
  td_authentication_id: ${td_authentication_ids.snowflake.default}
  account_name: [SNOWFLAKE_ACCOUNT]
  warehouse: [WAREHOUSE_NAME]
  db: [DATABASE_NAME]
  schema: ${datasource.schema}
  table: ${datasource.name}
  incremental: false

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

**Example**:
```yaml
in:
  type: snowflake
  td_authentication_id: ${td_authentication_ids.snowflake.default}
  account_name: RQRABIH-TREASUREDATA_PARTNER_BIZCRIT
  warehouse: VIPUL_COMPOSABLE_TEST
  schema: public
  db: INDRESH_TEST
  table: ORDERS
  incremental: false

filters:
- type: add_time
  to_column:
    name: time
    type: timestamp
  from_value:
    mode: upload_time

out:
  mode: append
```

---

### Type 2: Query-Based Load (With Incremental Column)

**File**: `config/snowflake_[table]_load.yml`

**EXACT TEMPLATE**:
```yaml
in:
  type: snowflake
  td_authentication_id: ${td_authentication_ids.snowflake.default}
  account_name: [SNOWFLAKE_ACCOUNT]
  warehouse: [WAREHOUSE_NAME]
  db: [DATABASE_NAME]
  schema: public
  source_type: query

  query: |
    SELECT *
    FROM ${datasource.schema}.${datasource.name}
    WHERE updated_at > '${td.last_results.start_time}'
      AND updated_at <= '${td.last_results.end_time}'

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

---

## Datasources Configuration

**File**: `config/snowflake_datasources.yml`

**EXACT TEMPLATE**:
```yaml
hist_datasources:
  - name: [table_name]
    table_name: snowflake_[table]_hist
    config_file: snowflake_[table]_load.yml
    incremental_field: [COLUMN_NAME]
    incremental_field_connector: [COLUMN_NAME]
    default_start_time: "2024-01-01 00:00:00.000"
    workflow_name: snowflake_ingest_hist.dig
    mode: append
    database: [DATABASE_NAME]
    schema: [SCHEMA_NAME]

inc_datasources:
  - name: [table_name]
    table_name: snowflake_[table]
    config_file: snowflake_[table]_load.yml
    incremental_field: [COLUMN_NAME]
    incremental_field_connector: [COLUMN_NAME]
    default_start_time: "2024-01-01 00:00:00.000"
    workflow_name: snowflake_ingest_inc.dig
    mode: append
    database: [DATABASE_NAME]
    schema: [SCHEMA_NAME]
```

**Example**:
```yaml
hist_datasources:
  - name: orders
    table_name: snowflake_orders_hist
    config_file: snowflake_orders_load.yml
    incremental_field: UPDATED_AT
    incremental_field_connector: UPDATED_AT
    default_start_time: "2024-01-01 00:00:00.000"
    workflow_name: snowflake_ingest_hist.dig
    mode: append
    database: demo_db
    schema: public

inc_datasources:
  - name: orders
    table_name: snowflake_orders
    config_file: snowflake_orders_load.yml
    incremental_field: UPDATED_AT
    incremental_field_connector: UPDATED_AT
    default_start_time: "2024-01-01 00:00:00.000"
    workflow_name: snowflake_ingest_inc.dig
    mode: append
    database: demo_db
    schema: public
```

---

## Authentication

### TD Authentication Setup

1. Create Snowflake authentication in TD console:
   - Go to Integrations > Authentications
   - Create new Snowflake authentication
   - Provide:
     - Username
     - Password
     - Account name
     - Database (optional)
   - Note the authentication ID

2. Add to `config/database.yml`:
```yaml
td_authentication_ids:
  snowflake:
    default: [AUTH_ID]
```

---

## Critical Snowflake-Specific Patterns

### 1. Account Name Format
- **CORRECT**: `account_name: RQRABIH-TREASUREDATA_PARTNER_BIZCRIT`
- **WRONG**: `account: RQRABIH-TREASUREDATA_PARTNER_BIZCRIT`

**Critical**: Snowflake connector uses `account_name`, NOT `account`

### 2. Database, Schema and Table
- **ALWAYS** specify Database and schema
- Table name should match Snowflake table exactly (case-sensitive)

### 3. Incremental Loading
- Set `incremental: false`
- Specify `incremental_column`
- Provide `from_value` and `to_value` from workflow

---

## Common Issues

### Issue: "Authentication failed"
**Cause**: Invalid credentials or authentication ID

**Solution**:
1. Verify authentication ID in TD console
2. Check credentials are correct
3. Ensure account_name matches Snowflake account

### Issue: "Table not found"
**Cause**: Incorrect table name or schema

**Solution**:
1. Verify table name is exact (case-sensitive)
2. Check schema is correct
3. Ensure database is accessible

### Issue: "Warehouse not available"
**Cause**: Warehouse suspended or name incorrect

**Solution**:
1. Verify warehouse name
2. Ensure warehouse is running in Snowflake
3. Check warehouse permissions

---

## Critical Reminders

1. **ALWAYS use `account_name`** (NOT `account`)
2. **Schema is required** (table alone is not sufficient)
3. **Case-sensitive** table and column names
4. **Incremental requires from_value/to_value** from workflow
5. **Database can be optional** if in authentication
6. **Use SQL Server timestamp format** (`yyyy-MM-dd HH:mm:ss.SSS`)
7. **Warehouse must be running** in Snowflake

---

## Reference

**Related Documentation**:
- Timestamp formats: `docs/patterns/timestamp-formats.md`
- Logging patterns: `docs/patterns/logging-patterns.md`
- Workflow patterns: `docs/patterns/workflow-patterns.md`

**Connector Documentation**:
- [Treasure Data Snowflake Connector](https://docs.treasuredata.com/display/public/INT/Snowflake+Import+Integration)
