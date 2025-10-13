-- Initial Load Staging Transformation for gbq_shopify_transaction
-- Source: mck_src.gbq_shopify_transaction_histunion
-- Target: mck_stg.gbq_shopify_transaction
-- Mode: Full Table Scan (Initial Load)

WITH cleaned_data AS (
  SELECT
    -- Keep incremental_date as-is per additional_rules
    incremental_date AS incremental_date,

    -- Numeric columns - cast to DOUBLE
    ROUND(TRY_CAST(amount AS DOUBLE), 2) AS amount,
    ROUND(TRY_CAST(currency_exchange_adjustment AS DOUBLE), 2) AS currency_exchange_adjustment,
    ROUND(TRY_CAST(currency_exchange_final_amount AS DOUBLE), 2) AS currency_exchange_final_amount,
    ROUND(TRY_CAST(currency_exchange_original_amount AS DOUBLE), 2) AS currency_exchange_original_amount,

    -- BIGINT columns - keep as-is
    currency_exchange_id AS currency_exchange_id,
    id AS id,
    location_id AS location_id,
    order_id AS order_id,
    parent_id AS parent_id,
    refund_id AS refund_id,
    test AS test,

    -- String columns - standardize in place
    NULLIF(NULLIF(NULLIF(NULLIF(TRIM(UPPER(authorization)), ''), 'NONE'), 'NULL'), 'N/A') AS authorization,
    NULLIF(NULLIF(NULLIF(NULLIF(TRIM(UPPER(currency)), ''), 'NONE'), 'NULL'), 'N/A') AS currency,
    NULLIF(NULLIF(NULLIF(NULLIF(TRIM(UPPER(currency_exchange_currency)), ''), 'NONE'), 'NULL'), 'N/A') AS currency_exchange_currency,
    NULLIF(NULLIF(NULLIF(NULLIF(TRIM(UPPER(device_id)), ''), 'NONE'), 'NULL'), 'N/A') AS device_id,
    NULLIF(NULLIF(NULLIF(NULLIF(TRIM(UPPER(error_code)), ''), 'NONE'), 'NULL'), 'N/A') AS error_code,
    NULLIF(NULLIF(NULLIF(NULLIF(TRIM(UPPER(gateway)), ''), 'NONE'), 'NULL'), 'N/A') AS gateway,
    NULLIF(NULLIF(NULLIF(NULLIF(TRIM(UPPER(kind)), ''), 'NONE'), 'NULL'), 'N/A') AS kind,
    NULLIF(NULLIF(NULLIF(NULLIF(TRIM(UPPER(message)), ''), 'NONE'), 'NULL'), 'N/A') AS message,
    NULLIF(NULLIF(NULLIF(NULLIF(TRIM(UPPER(payment_avs_result_code)), ''), 'NONE'), 'NULL'), 'N/A') AS payment_avs_result_code,
    NULLIF(NULLIF(NULLIF(NULLIF(TRIM(UPPER(payment_credit_card_bin)), ''), 'NONE'), 'NULL'), 'N/A') AS payment_credit_card_bin,
    NULLIF(NULLIF(NULLIF(NULLIF(TRIM(UPPER(payment_credit_card_company)), ''), 'NONE'), 'NULL'), 'N/A') AS payment_credit_card_company,
    NULLIF(NULLIF(NULLIF(NULLIF(TRIM(UPPER(payment_credit_card_number)), ''), 'NONE'), 'NULL'), 'N/A') AS payment_credit_card_number,
    NULLIF(NULLIF(NULLIF(NULLIF(TRIM(UPPER(payment_cvv_result_code)), ''), 'NONE'), 'NULL'), 'N/A') AS payment_cvv_result_code,
    NULLIF(NULLIF(NULLIF(NULLIF(TRIM(UPPER(source_name)), ''), 'NONE'), 'NULL'), 'N/A') AS source_name,
    NULLIF(NULLIF(NULLIF(NULLIF(TRIM(UPPER(status)), ''), 'NONE'), 'NULL'), 'N/A') AS status,
    NULLIF(NULLIF(NULLIF(NULLIF(TRIM(UPPER(user_id)), ''), 'NONE'), 'NULL'), 'N/A') AS user_id,
    NULLIF(NULLIF(NULLIF(NULLIF(TRIM(UPPER(shop_flavor_flag)), ''), 'NONE'), 'NULL'), 'N/A') AS shop_flavor_flag,

    -- Keep receipt JSON as-is per additional_rules (DO NOT TRANSFORM)
    receipt AS receipt,

    -- Date columns - 4 outputs each (original, _std, _unixtime, _date)
    -- authorization_expires_at
    authorization_expires_at AS authorization_expires_at,
    FORMAT_DATETIME(COALESCE(
      TRY_CAST(authorization_expires_at AS TIMESTAMP),
      FROM_UNIXTIME(TD_TIME_PARSE(authorization_expires_at)),
      TRY(DATE_PARSE(authorization_expires_at, '%d-%m-%Y %H:%i:%s.%f')),
      TRY(DATE_PARSE(authorization_expires_at, '%d-%m-%Y %H:%i:%s')),
      TRY(DATE_PARSE(authorization_expires_at, '%d-%m-%Y')),
      TRY(DATE_PARSE(authorization_expires_at, '%m/%d/%Y %H:%i:%s.%f')),
      TRY(DATE_PARSE(authorization_expires_at, '%m/%d/%Y %H:%i:%s')),
      TRY(DATE_PARSE(authorization_expires_at, '%m/%d/%Y')),
      TRY(FROM_ISO8601_TIMESTAMP(authorization_expires_at))
    ), 'yyyy-MM-dd HH:mm:ss') AS authorization_expires_at_std,
    TD_TIME_PARSE(FORMAT_DATETIME(COALESCE(
      TRY_CAST(authorization_expires_at AS TIMESTAMP),
      FROM_UNIXTIME(TD_TIME_PARSE(authorization_expires_at)),
      TRY(DATE_PARSE(authorization_expires_at, '%d-%m-%Y %H:%i:%s.%f')),
      TRY(DATE_PARSE(authorization_expires_at, '%d-%m-%Y %H:%i:%s')),
      TRY(DATE_PARSE(authorization_expires_at, '%d-%m-%Y')),
      TRY(DATE_PARSE(authorization_expires_at, '%m/%d/%Y %H:%i:%s.%f')),
      TRY(DATE_PARSE(authorization_expires_at, '%m/%d/%Y %H:%i:%s')),
      TRY(DATE_PARSE(authorization_expires_at, '%m/%d/%Y')),
      TRY(FROM_ISO8601_TIMESTAMP(authorization_expires_at))
    ), 'yyyy-MM-dd HH:mm:ss')) AS authorization_expires_at_unixtime,
    SUBSTR(FORMAT_DATETIME(COALESCE(
      TRY_CAST(authorization_expires_at AS TIMESTAMP),
      FROM_UNIXTIME(TD_TIME_PARSE(authorization_expires_at)),
      TRY(DATE_PARSE(authorization_expires_at, '%d-%m-%Y %H:%i:%s.%f')),
      TRY(DATE_PARSE(authorization_expires_at, '%d-%m-%Y %H:%i:%s')),
      TRY(DATE_PARSE(authorization_expires_at, '%d-%m-%Y')),
      TRY(DATE_PARSE(authorization_expires_at, '%m/%d/%Y %H:%i:%s.%f')),
      TRY(DATE_PARSE(authorization_expires_at, '%m/%d/%Y %H:%i:%s')),
      TRY(DATE_PARSE(authorization_expires_at, '%m/%d/%Y')),
      TRY(FROM_ISO8601_TIMESTAMP(authorization_expires_at))
    ), 'yyyy-MM-dd HH:mm:ss'), 1, 10) AS authorization_expires_at_date,

    -- created_at
    created_at AS created_at,
    FORMAT_DATETIME(COALESCE(
      TRY_CAST(created_at AS TIMESTAMP),
      FROM_UNIXTIME(TD_TIME_PARSE(created_at)),
      TRY(DATE_PARSE(created_at, '%d-%m-%Y %H:%i:%s.%f')),
      TRY(DATE_PARSE(created_at, '%d-%m-%Y %H:%i:%s')),
      TRY(DATE_PARSE(created_at, '%d-%m-%Y')),
      TRY(DATE_PARSE(created_at, '%m/%d/%Y %H:%i:%s.%f')),
      TRY(DATE_PARSE(created_at, '%m/%d/%Y %H:%i:%s')),
      TRY(DATE_PARSE(created_at, '%m/%d/%Y')),
      TRY(FROM_ISO8601_TIMESTAMP(created_at))
    ), 'yyyy-MM-dd HH:mm:ss') AS created_at_std,
    TD_TIME_PARSE(FORMAT_DATETIME(COALESCE(
      TRY_CAST(created_at AS TIMESTAMP),
      FROM_UNIXTIME(TD_TIME_PARSE(created_at)),
      TRY(DATE_PARSE(created_at, '%d-%m-%Y %H:%i:%s.%f')),
      TRY(DATE_PARSE(created_at, '%d-%m-%Y %H:%i:%s')),
      TRY(DATE_PARSE(created_at, '%d-%m-%Y')),
      TRY(DATE_PARSE(created_at, '%m/%d/%Y %H:%i:%s.%f')),
      TRY(DATE_PARSE(created_at, '%m/%d/%Y %H:%i:%s')),
      TRY(DATE_PARSE(created_at, '%m/%d/%Y')),
      TRY(FROM_ISO8601_TIMESTAMP(created_at))
    ), 'yyyy-MM-dd HH:mm:ss')) AS created_at_unixtime,
    SUBSTR(FORMAT_DATETIME(COALESCE(
      TRY_CAST(created_at AS TIMESTAMP),
      FROM_UNIXTIME(TD_TIME_PARSE(created_at)),
      TRY(DATE_PARSE(created_at, '%d-%m-%Y %H:%i:%s.%f')),
      TRY(DATE_PARSE(created_at, '%d-%m-%Y %H:%i:%s')),
      TRY(DATE_PARSE(created_at, '%d-%m-%Y')),
      TRY(DATE_PARSE(created_at, '%m/%d/%Y %H:%i:%s.%f')),
      TRY(DATE_PARSE(created_at, '%m/%d/%Y %H:%i:%s')),
      TRY(DATE_PARSE(created_at, '%m/%d/%Y')),
      TRY(FROM_ISO8601_TIMESTAMP(created_at))
    ), 'yyyy-MM-dd HH:mm:ss'), 1, 10) AS created_at_date,

    -- processed_at
    processed_at AS processed_at,
    FORMAT_DATETIME(COALESCE(
      TRY_CAST(processed_at AS TIMESTAMP),
      FROM_UNIXTIME(TD_TIME_PARSE(processed_at)),
      TRY(DATE_PARSE(processed_at, '%d-%m-%Y %H:%i:%s.%f')),
      TRY(DATE_PARSE(processed_at, '%d-%m-%Y %H:%i:%s')),
      TRY(DATE_PARSE(processed_at, '%d-%m-%Y')),
      TRY(DATE_PARSE(processed_at, '%m/%d/%Y %H:%i:%s.%f')),
      TRY(DATE_PARSE(processed_at, '%m/%d/%Y %H:%i:%s')),
      TRY(DATE_PARSE(processed_at, '%m/%d/%Y')),
      TRY(FROM_ISO8601_TIMESTAMP(processed_at))
    ), 'yyyy-MM-dd HH:mm:ss') AS processed_at_std,
    TD_TIME_PARSE(FORMAT_DATETIME(COALESCE(
      TRY_CAST(processed_at AS TIMESTAMP),
      FROM_UNIXTIME(TD_TIME_PARSE(processed_at)),
      TRY(DATE_PARSE(processed_at, '%d-%m-%Y %H:%i:%s.%f')),
      TRY(DATE_PARSE(processed_at, '%d-%m-%Y %H:%i:%s')),
      TRY(DATE_PARSE(processed_at, '%d-%m-%Y')),
      TRY(DATE_PARSE(processed_at, '%m/%d/%Y %H:%i:%s.%f')),
      TRY(DATE_PARSE(processed_at, '%m/%d/%Y %H:%i:%s')),
      TRY(DATE_PARSE(processed_at, '%m/%d/%Y')),
      TRY(FROM_ISO8601_TIMESTAMP(processed_at))
    ), 'yyyy-MM-dd HH:mm:ss')) AS processed_at_unixtime,
    SUBSTR(FORMAT_DATETIME(COALESCE(
      TRY_CAST(processed_at AS TIMESTAMP),
      FROM_UNIXTIME(TD_TIME_PARSE(processed_at)),
      TRY(DATE_PARSE(processed_at, '%d-%m-%Y %H:%i:%s.%f')),
      TRY(DATE_PARSE(processed_at, '%d-%m-%Y %H:%i:%s')),
      TRY(DATE_PARSE(processed_at, '%d-%m-%Y')),
      TRY(DATE_PARSE(processed_at, '%m/%d/%Y %H:%i:%s.%f')),
      TRY(DATE_PARSE(processed_at, '%m/%d/%Y %H:%i:%s')),
      TRY(DATE_PARSE(processed_at, '%m/%d/%Y')),
      TRY(FROM_ISO8601_TIMESTAMP(processed_at))
    ), 'yyyy-MM-dd HH:mm:ss'), 1, 10) AS processed_at_date,

    -- extended_authorization_extended_authorization_expires_at
    extended_authorization_extended_authorization_expires_at AS extended_authorization_extended_authorization_expires_at,
    FORMAT_DATETIME(COALESCE(
      TRY_CAST(extended_authorization_extended_authorization_expires_at AS TIMESTAMP),
      FROM_UNIXTIME(TD_TIME_PARSE(extended_authorization_extended_authorization_expires_at)),
      TRY(DATE_PARSE(extended_authorization_extended_authorization_expires_at, '%d-%m-%Y %H:%i:%s.%f')),
      TRY(DATE_PARSE(extended_authorization_extended_authorization_expires_at, '%d-%m-%Y %H:%i:%s')),
      TRY(DATE_PARSE(extended_authorization_extended_authorization_expires_at, '%d-%m-%Y')),
      TRY(DATE_PARSE(extended_authorization_extended_authorization_expires_at, '%m/%d/%Y %H:%i:%s.%f')),
      TRY(DATE_PARSE(extended_authorization_extended_authorization_expires_at, '%m/%d/%Y %H:%i:%s')),
      TRY(DATE_PARSE(extended_authorization_extended_authorization_expires_at, '%m/%d/%Y')),
      TRY(FROM_ISO8601_TIMESTAMP(extended_authorization_extended_authorization_expires_at))
    ), 'yyyy-MM-dd HH:mm:ss') AS extended_authorization_extended_authorization_expires_at_std,
    TD_TIME_PARSE(FORMAT_DATETIME(COALESCE(
      TRY_CAST(extended_authorization_extended_authorization_expires_at AS TIMESTAMP),
      FROM_UNIXTIME(TD_TIME_PARSE(extended_authorization_extended_authorization_expires_at)),
      TRY(DATE_PARSE(extended_authorization_extended_authorization_expires_at, '%d-%m-%Y %H:%i:%s.%f')),
      TRY(DATE_PARSE(extended_authorization_extended_authorization_expires_at, '%d-%m-%Y %H:%i:%s')),
      TRY(DATE_PARSE(extended_authorization_extended_authorization_expires_at, '%d-%m-%Y')),
      TRY(DATE_PARSE(extended_authorization_extended_authorization_expires_at, '%m/%d/%Y %H:%i:%s.%f')),
      TRY(DATE_PARSE(extended_authorization_extended_authorization_expires_at, '%m/%d/%Y %H:%i:%s')),
      TRY(DATE_PARSE(extended_authorization_extended_authorization_expires_at, '%m/%d/%Y')),
      TRY(FROM_ISO8601_TIMESTAMP(extended_authorization_extended_authorization_expires_at))
    ), 'yyyy-MM-dd HH:mm:ss')) AS extended_authorization_extended_authorization_expires_at_unixtime,
    SUBSTR(FORMAT_DATETIME(COALESCE(
      TRY_CAST(extended_authorization_extended_authorization_expires_at AS TIMESTAMP),
      FROM_UNIXTIME(TD_TIME_PARSE(extended_authorization_extended_authorization_expires_at)),
      TRY(DATE_PARSE(extended_authorization_extended_authorization_expires_at, '%d-%m-%Y %H:%i:%s.%f')),
      TRY(DATE_PARSE(extended_authorization_extended_authorization_expires_at, '%d-%m-%Y %H:%i:%s')),
      TRY(DATE_PARSE(extended_authorization_extended_authorization_expires_at, '%d-%m-%Y')),
      TRY(DATE_PARSE(extended_authorization_extended_authorization_expires_at, '%m/%d/%Y %H:%i:%s.%f')),
      TRY(DATE_PARSE(extended_authorization_extended_authorization_expires_at, '%m/%d/%Y %H:%i:%s')),
      TRY(DATE_PARSE(extended_authorization_extended_authorization_expires_at, '%m/%d/%Y')),
      TRY(FROM_ISO8601_TIMESTAMP(extended_authorization_extended_authorization_expires_at))
    ), 'yyyy-MM-dd HH:mm:ss'), 1, 10) AS extended_authorization_extended_authorization_expires_at_date,

    -- extended_authorization_standard_authorization_expires_at
    extended_authorization_standard_authorization_expires_at AS extended_authorization_standard_authorization_expires_at,
    FORMAT_DATETIME(COALESCE(
      TRY_CAST(extended_authorization_standard_authorization_expires_at AS TIMESTAMP),
      FROM_UNIXTIME(TD_TIME_PARSE(extended_authorization_standard_authorization_expires_at)),
      TRY(DATE_PARSE(extended_authorization_standard_authorization_expires_at, '%d-%m-%Y %H:%i:%s.%f')),
      TRY(DATE_PARSE(extended_authorization_standard_authorization_expires_at, '%d-%m-%Y %H:%i:%s')),
      TRY(DATE_PARSE(extended_authorization_standard_authorization_expires_at, '%d-%m-%Y')),
      TRY(DATE_PARSE(extended_authorization_standard_authorization_expires_at, '%m/%d/%Y %H:%i:%s.%f')),
      TRY(DATE_PARSE(extended_authorization_standard_authorization_expires_at, '%m/%d/%Y %H:%i:%s')),
      TRY(DATE_PARSE(extended_authorization_standard_authorization_expires_at, '%m/%d/%Y')),
      TRY(FROM_ISO8601_TIMESTAMP(extended_authorization_standard_authorization_expires_at))
    ), 'yyyy-MM-dd HH:mm:ss') AS extended_authorization_standard_authorization_expires_at_std,
    TD_TIME_PARSE(FORMAT_DATETIME(COALESCE(
      TRY_CAST(extended_authorization_standard_authorization_expires_at AS TIMESTAMP),
      FROM_UNIXTIME(TD_TIME_PARSE(extended_authorization_standard_authorization_expires_at)),
      TRY(DATE_PARSE(extended_authorization_standard_authorization_expires_at, '%d-%m-%Y %H:%i:%s.%f')),
      TRY(DATE_PARSE(extended_authorization_standard_authorization_expires_at, '%d-%m-%Y %H:%i:%s')),
      TRY(DATE_PARSE(extended_authorization_standard_authorization_expires_at, '%d-%m-%Y')),
      TRY(DATE_PARSE(extended_authorization_standard_authorization_expires_at, '%m/%d/%Y %H:%i:%s.%f')),
      TRY(DATE_PARSE(extended_authorization_standard_authorization_expires_at, '%m/%d/%Y %H:%i:%s')),
      TRY(DATE_PARSE(extended_authorization_standard_authorization_expires_at, '%m/%d/%Y')),
      TRY(FROM_ISO8601_TIMESTAMP(extended_authorization_standard_authorization_expires_at))
    ), 'yyyy-MM-dd HH:mm:ss')) AS extended_authorization_standard_authorization_expires_at_unixtime,
    SUBSTR(FORMAT_DATETIME(COALESCE(
      TRY_CAST(extended_authorization_standard_authorization_expires_at AS TIMESTAMP),
      FROM_UNIXTIME(TD_TIME_PARSE(extended_authorization_standard_authorization_expires_at)),
      TRY(DATE_PARSE(extended_authorization_standard_authorization_expires_at, '%d-%m-%Y %H:%i:%s.%f')),
      TRY(DATE_PARSE(extended_authorization_standard_authorization_expires_at, '%d-%m-%Y %H:%i:%s')),
      TRY(DATE_PARSE(extended_authorization_standard_authorization_expires_at, '%d-%m-%Y')),
      TRY(DATE_PARSE(extended_authorization_standard_authorization_expires_at, '%m/%d/%Y %H:%i:%s.%f')),
      TRY(DATE_PARSE(extended_authorization_standard_authorization_expires_at, '%m/%d/%Y %H:%i:%s')),
      TRY(DATE_PARSE(extended_authorization_standard_authorization_expires_at, '%m/%d/%Y')),
      TRY(FROM_ISO8601_TIMESTAMP(extended_authorization_standard_authorization_expires_at))
    ), 'yyyy-MM-dd HH:mm:ss'), 1, 10) AS extended_authorization_standard_authorization_expires_at_date,

    -- Keep time column as-is (no transformations - used for incremental processing)
    time AS time,

    -- Metadata columns
    'gbq_shopify_transaction_histunion' AS source_system,
    SUBSTR(CAST(CURRENT_TIMESTAMP AS VARCHAR), 1, 19) AS load_timestamp

  FROM mck_src.gbq_shopify_transaction_histunion
),
final_data AS (
  SELECT
    cleaned_data.*,
    ROW_NUMBER() OVER(
      PARTITION BY id
      ORDER BY incremental_date DESC, created_at_std DESC
    ) AS row_num
  FROM cleaned_data
)
SELECT
  incremental_date,
  amount,
  currency_exchange_adjustment,
  currency_exchange_final_amount,
  currency_exchange_original_amount,
  currency_exchange_id,
  id,
  location_id,
  order_id,
  parent_id,
  refund_id,
  test,
  authorization,
  currency,
  currency_exchange_currency,
  device_id,
  error_code,
  gateway,
  kind,
  message,
  payment_avs_result_code,
  payment_credit_card_bin,
  payment_credit_card_company,
  payment_credit_card_number,
  payment_cvv_result_code,
  source_name,
  status,
  user_id,
  shop_flavor_flag,
  receipt,
  authorization_expires_at,
  authorization_expires_at_std,
  authorization_expires_at_unixtime,
  authorization_expires_at_date,
  created_at,
  created_at_std,
  created_at_unixtime,
  created_at_date,
  processed_at,
  processed_at_std,
  processed_at_unixtime,
  processed_at_date,
  extended_authorization_extended_authorization_expires_at,
  extended_authorization_extended_authorization_expires_at_std,
  extended_authorization_extended_authorization_expires_at_unixtime,
  extended_authorization_extended_authorization_expires_at_date,
  extended_authorization_standard_authorization_expires_at,
  extended_authorization_standard_authorization_expires_at_std,
  extended_authorization_standard_authorization_expires_at_unixtime,
  extended_authorization_standard_authorization_expires_at_date,
  time,
  source_system,
  load_timestamp
FROM final_data
WHERE row_num = 1
