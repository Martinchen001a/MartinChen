{{ config(materialized='table') }}

WITH ads as (
  SELECT
    event_date,
    campaign_id,
    platform,
    impression,
    clicks,
    spend,
    purchases,
    purchase_value,
    reach,
    avg_frequency
  FROM {{ ref('fact_ads_daily_campaign') }}
),

crm as (
  SELECT
    order_date,
    campaign_id,
    orders_cnt,
    customers_cnt,
    revenue_clean,
    revenue_raw,
    missing_revenue_cnt,
    negative_revenue_cnt,
    revenue_outlier_cnt,
    normal_revenue_cnt
  FROM {{ ref('fact_crm_daily_campaign') }}
),

joined as (
  SELECT
    coalesce(a.event_date, c.order_date) as event_date,
    coalesce(a.campaign_id, c.campaign_id) as campaign_id,
    CASE 
        WHEN a.platform IS NOT NULL THEN a.platform
        WHEN c.campaign_id LIKE 'fb%' THEN 'facebook'
        WHEN c.campaign_id LIKE 'goog%' THEN 'google'
        ELSE 'organic'
    END AS platform,

    coalesce(a.impression, 0) as impression,
    coalesce(a.clicks, 0) as clicks,
    coalesce(a.spend, 0) as spend,
    coalesce(a.purchases, 0) as purchases,
    coalesce(a.purchase_value, 0) as purchase_value,
    coalesce(a.reach, 0) as reach,
    a.avg_frequency as avg_frequency,

    coalesce(c.orders_cnt, 0) as orders_cnt,
    coalesce(c.customers_cnt, 0) as customers_cnt,
    coalesce(c.revenue_clean, 0) as revenue_clean,
    coalesce(c.revenue_raw, 0) as revenue_raw,

    coalesce(c.missing_revenue_cnt, 0) as missing_revenue_cnt,
    coalesce(c.negative_revenue_cnt, 0) as negative_revenue_cnt,
    coalesce(c.revenue_outlier_cnt, 0) as revenue_outlier_cnt,
    coalesce(c.normal_revenue_cnt, 0) as normal_revenue_cnt
  FROM ads a
  FULL OUTER JOIN crm c
    on a.event_date = c.order_date
   and a.campaign_id = c.campaign_id
)

SELECT
	j.*,
	d.campaign_name,
	d.campaign_type,
	
	-- daily KPIs
	(revenue_clean::numeric / nullif(spend, 0)) as roas_daily,
	((revenue_clean - spend)::numeric / nullif(spend, 0)) as roi_daily,
	(spend::numeric / nullif(orders_cnt, 0)) as cpa_daily,
	CASE 
		WHEN spend = 0 and revenue_clean > 0 and j.campaign_id <> 'organic' THEN 'crm_only_conversion'
		WHEN spend > 0 and revenue_clean = 0 and orders_cnt = 0 THEN 'ads_only_spend'
		WHEN spend > 0 and revenue_clean > 0 THEN 'matched_same_day'
		WHEN j.campaign_id = 'organic' and revenue_clean > 0 THEN 'organic_conversion'
		ELSE 'other'
	END AS attribution_status

FROM joined j
LEFT JOIN {{ ref('dim_campaign') }} d
  on j.campaign_id = d.campaign_id
 and j.platform = d.platform
WHERE j.campaign_id <> 'organic'
