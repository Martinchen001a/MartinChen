{{ config(
    materialized='table'
) }}


SELECT DISTINCT ON (LOWER(TRIM(order_id)))
    LOWER(TRIM(order_id))::TEXT AS order_id,
	LOWER(TRIM(customer_id)) AS customer_id,
	CAST(order_date AS DATE) AS order_date,
	revenue::numeric AS revenue,
	LOWER(TRIM(channel_attributed)) AS channel_attributed,
	COALESCE(LOWER(TRIM(campaign_source)), 'organic') AS campaign_source,
	LOWER(TRIM(product_category)) AS product_category,
	LOWER(TRIM(region)) AS region,
	CASE 
        WHEN revenue::numeric >= 1000000 THEN 'revenue_outlier'
        WHEN revenue::numeric < 0 THEN 'negative_revenue'
        WHEN revenue::numeric IS NULL THEN 'missing_revenue'
        ELSE 'normal'
    END AS data_quality_tag,
	customer_id IS NULL AS has_missing_customer_id
FROM {{ source('stg_data', 'stg_crm_revenue') }}

