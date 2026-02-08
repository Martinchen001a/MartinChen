-- Assume region is shipping_region

{{ config(
    materialized='table'
) }}

WITH customer_status AS(
    SELECT DISTINCT 
    customer_id,
    region,
    ROW_NUMBER()OVER(PARTITION BY customer_id ORDER BY order_date DESC) AS rn
FROM {{ ref('transform_crm_revenue') }}
WHERE customer_id IS NOT NULL
)

SELECT DISTINCT 
    customer_id,  
    region
FROM customer_status
WHERE rn = 1