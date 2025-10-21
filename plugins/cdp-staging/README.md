# CDP Staging Plugin

**Production-ready data transformation from raw/histunion to staging layer**

---

## Overview

The CDP Staging plugin automates the transformation of raw or histunion tables into clean, standardized staging tables. It applies comprehensive data quality improvements, PII handling, JSON extraction, and deduplication strategies using either Presto or Hive SQL engines.

### Purpose

Transform raw ingested data into analytics-ready staging tables by automating:
- Data cleansing and standardization
- PII masking and handling
- JSON field extraction and flattening
- Deduplication strategies
- Data type conversions
- Quality validation
- Schema-driven transformations

---

## Features

### Schema-Driven Transformation
- **Live Schema Analysis**: Uses MCP to fetch exact table schemas from TD
- **Automatic Column Detection**: No manual column listing required
- **Data Type Intelligence**: Proper type casting and conversions
- **Null Handling**: Smart NULL value management

### Data Quality Improvements
- **Trimming**: Remove leading/trailing whitespace
- **Case Normalization**: LOWER() for emails, standardized casing
- **Email Validation**: Pattern matching and standardization
- **Phone Standardization**: Format normalization
- **Date/Time Handling**: Proper timestamp conversions
- **Numeric Validation**: Range checks and casting

### PII Handling
- **Email Masking**: Configurable masking strategies
- **Phone Number Masking**: Partial masking (last 4 digits)
- **Address Anonymization**: Optional field masking
- **GDPR Compliance**: Support for data privacy requirements

### JSON Extraction
- **Nested Field Extraction**: Pull out JSON fields to columns
- **Array Flattening**: Convert JSON arrays to rows/columns
- **Type Casting**: Proper data types for extracted fields
- **Error Handling**: Safe extraction with NULL fallbacks

### Deduplication
- **ROW_NUMBER Strategies**: Keep first/last based on criteria
- **Timestamp-Based**: Most recent record wins
- **Custom Logic**: Flexible deduplication rules
- **Composite Keys**: Multi-column uniqueness

### Multi-Engine Support
- **Presto SQL**: Default engine for most transformations
- **Hive SQL**: Alternative for specific use cases
- **Engine-Specific Optimizations**: Leverages each engine's strengths

---

## Slash Commands

### `/cdp-staging:transform-table`

Transform a single table from histunion to staging format.

**Usage:**
```bash
/cdp-staging:transform-table
```

**Prompts for:**
- Source database and table (e.g., `client_src.klaviyo_events_histunion`)
- Target database (e.g., `client_stg`)
- SQL Engine (Presto or Hive)
- Deduplication strategy
- PII handling preferences

**Generates:**
- `staging/queries/{table}.sql` - Transformation SQL
- `staging/{table}.dig` - Workflow file (optional)

**Example Output:**
```sql
-- File: staging/queries/klaviyo_events.sql

INSERT OVERWRITE TABLE client_stg.klaviyo_events
SELECT
    -- ID fields
    TRIM(event_id) AS event_id,
    TRIM(profile_id) AS profile_id,

    -- Email with PII handling
    LOWER(TRIM(email)) AS email,

    -- Timestamps
    CAST(timestamp AS BIGINT) AS event_timestamp,
    FROM_UNIXTIME(timestamp) AS event_datetime,

    -- JSON extraction
    JSON_EXTRACT_SCALAR(properties, '$.product_id') AS product_id,
    JSON_EXTRACT_SCALAR(properties, '$.revenue') AS revenue,

    -- Metadata
    time AS td_time,
    CURRENT_TIMESTAMP AS processed_at

FROM client_src.klaviyo_events_histunion

WHERE time > (
    SELECT COALESCE(MAX(td_time), 0)
    FROM client_stg.klaviyo_events
)

-- Deduplication
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY event_id
    ORDER BY timestamp DESC
) = 1
```

---

### `/cdp-staging:transform-batch`

Transform multiple tables in parallel for maximum efficiency.

**Usage:**
```bash
/cdp-staging:transform-batch
```

**Prompts for:**
- List of tables (comma-separated or one per line)
- Source database
- Target database
- SQL Engine (Presto or Hive)
- Common transformation rules

**Example Input:**
```
Tables to transform:
client_src.klaviyo_events_histunion
client_src.klaviyo_profiles_histunion
client_src.shopify_orders_histunion
client_src.shopify_products_histunion
```

**Generates:**
- One SQL file per table in `staging/queries/`
- Master workflow with parallel execution
- Dependency management between tables
- Comprehensive logging

**Batch Workflow:**
```yaml
# staging/batch_staging_transform.dig

_parallel: true

+transform_klaviyo_events:
  td>: staging/queries/klaviyo_events.sql
  database: client_stg

+transform_klaviyo_profiles:
  td>: staging/queries/klaviyo_profiles.sql
  database: client_stg

+transform_shopify_orders:
  td>: staging/queries/shopify_orders.sql
  database: client_stg

+transform_shopify_products:
  td>: staging/queries/shopify_products.sql
  database: client_stg
```

---

### `/cdp-staging:transform-validation`

Validate generated staging SQL against CLAUDE.md compliance and quality gates.

**Usage:**
```bash
/cdp-staging:transform-validation
```

**Validates:**
- SQL syntax correctness (Presto/Hive)
- Required transformations present
- Data quality checks included
- PII handling compliance
- Deduplication logic
- NULL handling
- Performance optimizations
- Naming conventions

**Output:**
```
Validating: staging/queries/klaviyo_events.sql

✓ SQL syntax valid (Presto)
✓ Data cleansing (TRIM) applied
✓ Email standardization (LOWER) present
✓ Deduplication logic found
✓ Incremental load pattern detected
✓ Metadata columns included
⚠ Warning: Consider adding INDEX on event_id
✓ PII handling compliant
✓ CLAUDE.md quality gates passed

Overall: PASS (1 warning)
```

---

## Transformation Patterns

### Pattern 1: Basic Cleansing

```sql
SELECT
    -- String trimming and case normalization
    TRIM(customer_id) AS customer_id,
    LOWER(TRIM(email)) AS email,
    UPPER(TRIM(country_code)) AS country_code,

    -- NULL handling
    COALESCE(first_name, 'Unknown') AS first_name,
    COALESCE(status, 'active') AS status,

    -- Metadata
    time AS td_time,
    CURRENT_TIMESTAMP AS processed_at

FROM source_table
```

### Pattern 2: JSON Extraction

```sql
SELECT
    event_id,

    -- Extract scalar values
    JSON_EXTRACT_SCALAR(properties, '$.product_id') AS product_id,
    JSON_EXTRACT_SCALAR(properties, '$.category') AS category,

    -- Extract and cast
    CAST(JSON_EXTRACT_SCALAR(properties, '$.price') AS DOUBLE) AS price,
    CAST(JSON_EXTRACT_SCALAR(properties, '$.quantity') AS INTEGER) AS quantity,

    -- Safe extraction with fallback
    COALESCE(
        JSON_EXTRACT_SCALAR(properties, '$.discount'),
        '0'
    ) AS discount,

    -- Keep original for reference
    properties AS raw_properties

FROM events_table
```

### Pattern 3: Deduplication

```sql
SELECT *
FROM (
    SELECT
        customer_id,
        email,
        first_name,
        updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY updated_at DESC, time DESC
        ) AS row_num
    FROM customer_table
)
WHERE row_num = 1
```

**Hive Equivalent:**
```sql
INSERT OVERWRITE TABLE staging_table
SELECT customer_id, email, first_name, updated_at
FROM (
    SELECT
        customer_id,
        email,
        first_name,
        updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY updated_at DESC, time DESC
        ) AS row_num
    FROM customer_table
) ranked
WHERE row_num = 1
```

### Pattern 4: PII Masking

```sql
SELECT
    customer_id,

    -- Email masking
    CASE
        WHEN consent_given = true THEN email
        ELSE CONCAT(
            SUBSTR(email, 1, 3),
            '***@',
            SPLIT_PART(email, '@', 2)
        )
    END AS email_masked,

    -- Phone masking (show last 4 digits)
    CASE
        WHEN consent_given = true THEN phone
        ELSE CONCAT('***-***-', SUBSTR(phone, -4))
    END AS phone_masked,

    -- Address anonymization
    CASE
        WHEN consent_given = true THEN address
        ELSE NULL
    END AS address

FROM customer_profiles
```

### Pattern 5: Incremental Load

```sql
INSERT INTO client_stg.orders
SELECT
    order_id,
    customer_id,
    order_date,
    total_amount,
    time AS td_time,
    CURRENT_TIMESTAMP AS processed_at

FROM client_src.orders_histunion

WHERE time > (
    SELECT COALESCE(MAX(td_time), 0)
    FROM client_stg.orders
)

-- Deduplicate within this batch
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_id
    ORDER BY time DESC
) = 1
```

---

## Presto vs Hive SQL

### When to Use Presto

**Advantages:**
- Faster query execution
- Better for interactive queries
- Modern SQL syntax support
- `QUALIFY` clause for easy deduplication
- Better JSON handling

**Best For:**
- Real-time transformations
- Complex JSON extraction
- Interactive development
- Window functions with QUALIFY

### When to Use Hive

**Advantages:**
- Better for very large datasets
- More stable for batch processing
- Lower resource consumption
- Better for INSERT OVERWRITE patterns

**Best For:**
- Batch ETL processes
- Large table transformations
- Resource-constrained environments
- Scheduled workflows

### Key Syntax Differences

| Feature | Presto | Hive |
|---------|--------|------|
| Deduplication | `QUALIFY ROW_NUMBER()` | Subquery with `WHERE row_num = 1` |
| JSON Extract | `JSON_EXTRACT_SCALAR()` | `GET_JSON_OBJECT()` |
| String Split | `SPLIT_PART()` | `SPLIT()[index]` |
| Timestamp | `FROM_UNIXTIME()` | `FROM_UNIXTIME()` |
| Insert Mode | `INSERT INTO` | `INSERT OVERWRITE TABLE` |

---

## Data Quality Checks

### Email Validation

```sql
SELECT
    email,
    -- Standardize
    LOWER(TRIM(email)) AS email_clean,

    -- Validate format
    CASE
        WHEN REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z]{2,}$')
        THEN true
        ELSE false
    END AS email_valid,

    -- Flag test emails
    CASE
        WHEN email LIKE '%@test.com' OR email LIKE '%@example.com'
        THEN true
        ELSE false
    END AS is_test_email

FROM profiles
```

### Numeric Range Validation

```sql
SELECT
    order_id,
    total_amount,

    -- Validate ranges
    CASE
        WHEN total_amount < 0 THEN NULL  -- Invalid negative
        WHEN total_amount > 1000000 THEN NULL  -- Suspicious high value
        ELSE total_amount
    END AS total_amount_validated,

    -- Flag for review
    CASE
        WHEN total_amount < 0 OR total_amount > 1000000
        THEN 'needs_review'
        ELSE 'valid'
    END AS validation_status

FROM orders
```

### Date Validation

```sql
SELECT
    event_id,
    event_date,

    -- Validate date range
    CASE
        WHEN event_date < DATE '2020-01-01' THEN NULL  -- Too old
        WHEN event_date > CURRENT_DATE THEN NULL  -- Future date
        ELSE event_date
    END AS event_date_validated,

    -- Convert to timestamp
    CAST(event_date AS TIMESTAMP) AS event_timestamp

FROM events
```

---

## Usage Examples

### Example 1: Transform Klaviyo Events

```bash
/cdp-staging:transform-table
```

**Input:**
- Source: `client_src.klaviyo_events_histunion`
- Target DB: `client_stg`
- Engine: Presto
- Dedup: By event_id, keep latest

**Generated SQL:**
```sql
INSERT INTO client_stg.klaviyo_events
SELECT
    TRIM(event_id) AS event_id,
    TRIM(profile_id) AS profile_id,
    LOWER(TRIM(email)) AS email,
    TRIM(event_name) AS event_name,

    -- JSON extraction
    JSON_EXTRACT_SCALAR(properties, '$.product_name') AS product_name,
    CAST(JSON_EXTRACT_SCALAR(properties, '$.price') AS DOUBLE) AS price,

    -- Timestamps
    CAST(timestamp AS BIGINT) AS event_timestamp,
    FROM_UNIXTIME(timestamp) AS event_datetime,

    -- Metadata
    time AS td_time,
    CURRENT_TIMESTAMP AS processed_at

FROM client_src.klaviyo_events_histunion

WHERE time > (
    SELECT COALESCE(MAX(td_time), 0)
    FROM client_stg.klaviyo_events
)

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY event_id
    ORDER BY timestamp DESC, time DESC
) = 1
```

---

### Example 2: Batch Transform with PII

```bash
/cdp-staging:transform-batch
```

**Input:**
```
Tables:
client_src.customer_profiles_histunion
client_src.customer_orders_histunion
client_src.customer_interactions_histunion

PII Handling: Mask emails for non-consented users
Engine: Presto
```

**Generated:**
- 3 SQL files with consistent PII handling
- Parallel execution workflow
- Consent-aware masking logic

---

### Example 3: Hive Transformation for Large Dataset

```bash
/cdp-staging:transform-table
```

**Input:**
- Source: `client_src.web_logs_histunion` (500M+ rows)
- Engine: Hive
- Dedup: By session_id, latest timestamp

**Generated Hive SQL:**
```sql
INSERT OVERWRITE TABLE client_stg.web_logs
SELECT
    session_id,
    user_id,
    page_url,
    visit_timestamp,
    time AS td_time
FROM (
    SELECT
        session_id,
        user_id,
        page_url,
        visit_timestamp,
        time,
        ROW_NUMBER() OVER (
            PARTITION BY session_id
            ORDER BY visit_timestamp DESC
        ) AS row_num
    FROM client_src.web_logs_histunion
    WHERE time > ${last_processed_time}
) ranked
WHERE row_num = 1
```

---

## Best Practices

### 1. Schema Validation
- Always use MCP to get current schemas
- Don't hardcode column lists
- Verify data types before transformation
- Test with sample data first

### 2. Incremental Loading
- Use `td_time` for watermarking
- Always include WHERE clause for incremental
- Handle late-arriving data appropriately
- Monitor watermark progression

### 3. Performance Optimization
- Use QUALIFY instead of subqueries (Presto)
- Partition large tables appropriately
- Limit JSON extraction to needed fields
- Consider materialized views for complex transforms

### 4. Data Quality
- Always apply TRIM to string fields
- Standardize email to lowercase
- Validate formats before saving
- Log quality metrics

### 5. PII Compliance
- Mask PII fields by default
- Respect consent flags
- Document PII handling in comments
- Audit PII access regularly

### 6. Error Handling
- Use COALESCE for NULL defaults
- Safe casting with TRY_CAST
- Log transformation errors
- Monitor failed transformations

---

## File Structure

```
staging/
├── queries/
│   ├── klaviyo_events.sql
│   ├── klaviyo_profiles.sql
│   ├── shopify_orders.sql
│   └── shopify_products.sql
│
├── batch_staging_transform.dig
├── klaviyo_events.dig
└── validation_report.txt
```

---

## Common Issues

### Issue: Column Not Found

**Cause:** Schema changed since last check

**Solution:**
```bash
# Re-fetch schema using MCP
/cdp-staging:transform-table
# Agent will get latest schema automatically
```

### Issue: Deduplication Not Working

**Cause:** Incorrect partition key or order

**Solution:**
```sql
-- Verify unique key
SELECT customer_id, COUNT(*)
FROM staging_table
GROUP BY customer_id
HAVING COUNT(*) > 1

-- Fix PARTITION BY clause
PARTITION BY correct_unique_key
ORDER BY timestamp DESC, time DESC  -- Add tie-breaker
```

### Issue: JSON Extraction Returns NULL

**Cause:** Incorrect JSON path or invalid JSON

**Solution:**
```sql
-- Debug JSON structure
SELECT properties FROM source_table LIMIT 5

-- Use safe extraction
COALESCE(
    TRY(JSON_EXTRACT_SCALAR(properties, '$.field')),
    'default_value'
) AS field
```

---

## Quality Gates

All transformations must include:

1. **Data Cleansing**: TRIM on all string fields
2. **Email Standardization**: LOWER() on email fields
3. **Deduplication**: ROW_NUMBER or equivalent
4. **Incremental Logic**: Watermark-based filtering
5. **Metadata Columns**: td_time, processed_at
6. **NULL Handling**: COALESCE where appropriate
7. **Type Casting**: Proper data types
8. **Comments**: Transformation logic documented

---

## Support

For assistance:
- Review CLAUDE.md in plugin directory
- Check validation output
- Examine generated SQL in `staging/queries/`
- Verify source schema using MCP tools

---

**Version:** 1.2.0
**Last Updated:** 2024-10-13
**Maintained by:** APS CDP Team
