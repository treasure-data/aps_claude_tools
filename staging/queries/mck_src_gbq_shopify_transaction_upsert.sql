-- Upsert SQL for gbq_shopify_transaction
-- Purpose: Delete existing records and insert new records from work table
-- Partition Column: id
-- CRITICAL: This file is for Digdag execution ONLY - DO NOT execute directly

DELETE FROM ${staging_database}.${table.staging_table}
WHERE id IN (
    SELECT id FROM ${staging_database}.work_${table.staging_table}
    WHERE id IS NOT NULL
);

INSERT INTO ${staging_database}.${table.staging_table}
SELECT * FROM ${staging_database}.work_${table.staging_table};
