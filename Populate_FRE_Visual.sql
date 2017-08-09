CREATE VIEW tmp_fraud_recommendation_test AS
SELECT
advertiser_name,
advertiser_id,
publisher_name as partner,
publisher_id,
site_name as app,
site_id,
traffic_week,
Fraud_Pattern as pattern,
CASE WHEN length(country_name) = 0 and length(publisher_sub_publisher) = 0 and length(country_sub_publisher) = 0 and
                length(publisher_sub_campaign) = 0 then 'Partner'
WHEN length(country_name) > 0 and length(publisher_sub_publisher) = 0 then 'Country'
WHEN length(publisher_sub_campaign) > 0 THEN 'Partner Campaign'
WHEN length(country_name) = 0 and length(publisher_sub_publisher) > 0 THEN 'Partner Publisher'
WHEN length(country_sub_publisher) > 0 THEN 'Country + Partner Publisher'
END AS level,
CASE WHEN (COALESCE(NULLIF(country_name,''), NULLIF(publisher_sub_publisher,''),NULLIF(publisher_sub_campaign,''),NULLIF(country_sub_publisher,''))) IS NULL THEN publisher_name
ELSE (COALESCE(NULLIF(country_name,''), NULLIF(publisher_sub_publisher,''),NULLIF(publisher_sub_campaign,''),NULLIF(country_sub_publisher,''))) END AS level_name,
subset_qty as level_flagged_installs,
CASE time_interval
    WHEN 'First 5 Minutes' THEN 20*val
    WHEN 'First Hour' THEN 5*val
    ELSE val
END
AS interval_time,
split_values as interval_installs,
time_interval as interval_flagged,
Flagged_Install as reco_flagged_installs,
Total_Install as reco_total_installs,
CASE
WHEN LOWER(publisher_name) = 'youtube'  THEN  'Monitor Traffic'
WHEN (Flagged_Install*1.0)/Total_Install > .35 THEN 'Review All Traffic'
WHEN (Flagged_Install*1.0)/Total_Install > .05 THEN 'Review Subset(s)'
WHEN LOWER(publisher_name) = 'youtube'  THEN  'Monitor Traffic'
ELSE 'Monitor Traffic'
END AS
Recommendation
FROM
(

SELECT
  advertiser_name,
  advertiser_id,
  publisher_name,
  publisher_id,
  site_name,
  site_id,
  traffic_week,
  'Linear' as Fraud_Pattern,
  country_name,
  publisher_sub_publisher,
  publisher_sub_campaign,
  country_sub_publisher,
  subset_qty,
val,
  CASE time_interval when 'First 5 Minutes' then split_qty_per_20_second
    when 'First Hour' then split_qty_per_5_minute
    when 'First 24 Hours' then split_qty_per_hour
    END as split_values,
time_interval,
  greatest(
  sum(case when length(country_name) = 0 and length(publisher_sub_publisher) = 0 and length(country_sub_publisher) = 0 and
                length(publisher_sub_campaign) = 0 then subset_qty else 0 end )over (partition by publisher_name, site_name,traffic_week),
  sum(case when length(country_name) > 0 and length(publisher_sub_publisher) = 0 then subset_qty else 0 end ) over (partition by publisher_name, site_name,traffic_week),
  sum(case when length(publisher_sub_campaign) > 0 then subset_qty else 0 end ) over (partition by publisher_name, site_name,traffic_week),
  sum(case when length(country_name) = 0 and length(publisher_sub_publisher) > 0 then subset_qty else 0 end ) over (partition by publisher_name, site_name,traffic_week),
  sum(case when length(country_sub_publisher) > 0 then subset_qty else 0 end ) over (partition by publisher_name, site_name,traffic_week)
    )/24 as Flagged_Install,
  max(adv_site_pub_wk_qty) as Total_Install
from tmp_fraud_recommendation_split_time
where length(linear_exception)>0
and traffic_week >= '20170101'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16

UNION

SELECT
  advertiser_name,
  advertiser_id,
  publisher_name,
  publisher_id,
  site_name,
  site_id,
  traffic_week,
  'Click Injection' as Fraud_Pattern,
  country_name,
  publisher_sub_publisher,
  publisher_sub_campaign,
  country_sub_publisher,
  subset_qty,
  val,
split_qty_per_20_second as split_values,
 'First 5 Minutes' as time_interval,
  greatest(
  sum(case when length(country_name) = 0 and length(publisher_sub_publisher) = 0 and length(country_sub_publisher) = 0 and
                length(publisher_sub_campaign) = 0 then subset_qty else 0 end )over (partition by publisher_name, site_name,traffic_week),
  sum(case when length(country_name) > 0 and length(publisher_sub_publisher) = 0 then subset_qty else 0 end ) over (partition by publisher_name, site_name,traffic_week),
  sum(case when length(publisher_sub_campaign) > 0 then subset_qty else 0 end ) over (partition by publisher_name, site_name,traffic_week),
  sum(case when length(country_name) = 0 and length(publisher_sub_publisher) > 0 then subset_qty else 0 end ) over (partition by publisher_name, site_name,traffic_week),
  sum(case when length(country_sub_publisher) > 0 then subset_qty else 0 end ) over (partition by publisher_name, site_name,traffic_week)
   )/24 as Flagged_Install,
  max(adv_site_pub_wk_qty) as Total_Install
from tmp_fraud_recommendation_split_time
where length(hijack_exception)>0
and traffic_week >= '20170101'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
)
data
where split_values is not null
ORDER BY 1,2,3,4,5,6,7,8,9,10,11,12;
