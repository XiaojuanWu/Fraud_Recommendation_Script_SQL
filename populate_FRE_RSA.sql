--  select traffic_week, count(*), count(distinct(advertiser_name)) from tmp_Fraud_weekly_Recommendation group by 1 order by 1;

-- Master script to populate fraud_weekly_recommendations ---------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Advertiser, Site, Publisher level detail
-- ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
delete from tmp_fraud_weekly_recommendation
where traffic_week >= '2017-05-22' and traffic_week < '2017-05-29';
commit;

insert /* JT dev RSA for Uber Fraud Pilot */ into tmp_fraud_weekly_recommendation
SELECT  advertiser_name, advertiser_id, site_name, site_id, publisher_name, publisher_id, traffic_week, max(pub_qty) pub_qty,
  '' publisher_sub_campaign, '' publisher_sub_publisher,'' country_name, '' Country_Sub_publisher,
  count(*) install_qty,
  round(count(*) * 100 / max(pub_qty) ::decimal,2) pct_of_traffic,
    max(case when qtile=1 then second_diff else null end) q1_time,
    max(case when qtile=2 then second_diff else null end) - min(case when qtile=2 then second_diff else null end) q2_time,
    max(case when qtile=3 then second_diff else null end) - min(case when qtile=3 then second_diff else null end) q3_time,
    max(case when qtile=4 then second_diff else null end) - min(case when qtile=4 then second_diff else null end) q4_time,
    round( ( max(case when qtile=2 then second_diff else null end) - min(case when qtile=2 then second_diff else null end) ) /
                    ( max(case when qtile=3 then second_diff else null end) - min(case when qtile=3 then second_diff else null end) )  ::decimal,3) ratio_Q2_over_Q3,
    case when round( ( max(case when qtile=2 then second_diff else null end) - min(case when qtile=2 then second_diff else null end) ) /
                    ( max(case when qtile=3 then second_diff else null end) - min(case when qtile=3 then second_diff else null end) )  ::decimal,3) > 0.9
         then 'linear trend appears' ELSE '' END linear_exception,
    case when max(case when qtile=1 then second_diff else null end) <= 30
         then '25% OF traffic WITHIN ' || max(case when qtile=1 then second_diff else null end) || ' seconds' ELSE '' END hijack_exception,
            sum(case when second_diff between  0 and   20 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 21 and   40 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 41 and   60 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 61 and   80 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 81 and  100 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 101 and 120 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 121 and 140 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 141 and 160 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 161 and 180 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 181 and 200 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 201 and 220 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 221 and 240 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 241 and 260 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 261 and 280 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 281 and 300 then 1 else 0 end )  Qty_Per_20_second,
           sum(case when second_diff between 0 and 300 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 301 and 600 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 601 and 900 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 901 and 1200then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 1201 and 1200 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 1501 and 1800 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 1801 and 2100 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 2101 and 2400 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 2401 and 2700 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 2701 and 3000 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 3001 and 3300 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 3301 and 3600 then 1 else 0 end ) Qty_Per_5_minute,
           sum(case when second_diff between     0 and  3600 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between  3601 and  7200 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between  7201 and 10800 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 10801 and 14400 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 14401 and 18000 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 18001 and 21600 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 21601 and 25200 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 25201 and 28800 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 28801 and 32400 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 32401 and 36000 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 36001 and 39600 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 39601 and 43200 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 43201 and 46800 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 46801 and 50400 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 50401 and 54000 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 54001 and 57600 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 57601 and 61200 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 61201 and 64800 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 64801 and 68400 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 68401 and 72000 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 72001 and 75600 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 75601 and 79200 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 79201 and 82800 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 82801 and 86400 then 1 else 0 end )  Qty_Per_hour,
    round(sum(case when device_type = 'aphone' then 1 else 0 end) * 100 /count(*)::decimal,1)  pct_aPhone,
    round(sum(case when device_type = 'iphone' then 1 else 0 end) * 100 /count(*)::decimal,1)  pct_iPhone,
    round(sum(case when device_type = 'atablet' then 1 else 0 end) * 100 /count(*)::decimal,1)  pct_aTab,
    round(sum(case when device_type = 'itablet' then 1 else 0 end) * 100 /count(*)::decimal,1)  pct_iTab,
    round( sum( case when match_type = 'fingerpr' then 1 else 0 end) * 100 / count(*) :: decimal,1) pct_fingerprint,
    round( sum( case when match_type = 'deviceid' then 1 else 0 end) * 100 / count(*) :: decimal,1) pct_deviceid,
    round( sum( case when match_type = 'referrer' then 1 else 0 end) * 100 / count(*) :: decimal,1) pct_referrer,
    round( sum( case when match_type = 'inline' then 1 else 0 end) * 100 / count(*) :: decimal,1) pct_inline,
    round( sum( case when match_type = 'preload' then 1 else 0 end) * 100 / count(*) :: decimal,1) pct_preload,
    round( sum( case when match_type not in ('fingerpr','deviceid','referrer','inline','preload') then 1 else 0 end) * 100 / count(*) :: decimal,1) pct_other,
    round( sum( case status_code when  0 then 1 else 0 end ) * 100 / count(*) :: decimal,1) pct_approved,
    round( sum( case status_code when 81 then 1 else 0 end ) * 100 / count(*) :: decimal,1) pct_app_30_day,
    round( sum( case status_code when 80 then 1 else 0 end ) * 100 / count(*) :: decimal,1) pct_app_reinstall,
    round( sum( case status_code when 50 then 1 else 0 end ) * 100 / count(*) :: decimal,1) pct_rej_jailbroke,
    round( sum( case status_code when 51 then 1 else 0 end ) * 100 / count(*) :: decimal,1) pct_rej_invalid,
    round( sum( case status_code when 72 then 1 else 0 end ) * 100 / count(*) :: decimal,1) pct_rej_unattr,
    round( sum( case status_code when 73 then 1 else 0 end ) * 100 / count(*) :: decimal,1) pct_rej_already
    FROM (
    SELECT date(date_trunc('week', log_date_hour)) traffic_week, advertiser_name, advertiser_id, site_name, site_id, publisher_name, publisher_id,
        second_diff, count(1) over(PARTITION by date(date_trunc('week', log_date_hour)), advertiser_name, site_name, publisher_name) pub_qty,
        ntile(4) OVER (PARTITION by date(date_trunc('week', log_date_hour)), advertiser_name,site_name, publisher_name ORDER BY second_diff ) qtile, device_type, match_type, log_status, status_code
        FROM tmc_fraud_lag_install_detailed
        WHERE log_Date_hour >= '2017-05-22' and log_date_hour < '2017-05-29'
            and attributable_type = 'click'
            and match_type <> 'click' AND second_diff <= 24*60*60 and installs > 0
            -- removing TUNE-generated clicks
            and
            (case when match_type = 'preload'
            or (match_type = 'inline' AND publisher_name like 'Facebook%')
            or (match_type = 'referrer' AND publisher_name IN('Facebook Organic', 'Google Organic'))
            then 1 else 0 end) = 0
        ) a
    GROUP BY 1,2,3,4,5,6,7,9,10,11,12
    HAVING max(case when qtile=2 then second_diff else null end) - min(case when qtile=2 then second_diff else null end) >0
       AND max(case when qtile=3 then second_diff else null end) - min(case when qtile=3 then second_diff else null end) >0
       AND ( round( ( max(case when qtile=2 then second_diff else null end) - min(case when qtile=2 then second_diff else null end) ) /
                    ( max(case when qtile=3 then second_diff else null end) - min(case when qtile=3 then second_diff else null end) )  ::decimal,3)  > 0.9
       OR max(case when qtile=1 then second_diff else null end) <= 30 )
       AND count(*) > 200
UNION ALL
-- Add publisher_sub_campaign to group by ------------------------------------------------------------------------------
SELECT  advertiser_name, advertiser_id, site_name, site_id, publisher_name, publisher_id, traffic_week, max(pub_qty) pub_qty, publisher_sub_campaign, '' publisher_sub_publisher,'' country_name, '' Country_Sub_publisher,
  count(*) install_qty,
  round(count(*) * 100 / max(pub_qty) ::decimal,2) pct_of_traffic,
    max(case when qtile=1 then second_diff else null end) q1_time,
    max(case when qtile=2 then second_diff else null end) - min(case when qtile=2 then second_diff else null end) q2_time,
    max(case when qtile=3 then second_diff else null end) - min(case when qtile=3 then second_diff else null end) q3_time,
    max(case when qtile=4 then second_diff else null end) - min(case when qtile=4 then second_diff else null end) q4_time,
    round( ( max(case when qtile=2 then second_diff else null end) - min(case when qtile=2 then second_diff else null end) ) /
                    ( max(case when qtile=3 then second_diff else null end) - min(case when qtile=3 then second_diff else null end) )  ::decimal,3) ratio_Q2_over_Q3,
    case when round( ( max(case when qtile=2 then second_diff else null end) - min(case when qtile=2 then second_diff else null end) ) /
                    ( max(case when qtile=3 then second_diff else null end) - min(case when qtile=3 then second_diff else null end) )  ::decimal,3) > 0.9
         then 'linear trend appears' ELSE '' END linear_exception,
    case when max(case when qtile=1 then second_diff else null end) <= 30
         then '25% OF traffic WITHIN ' || max(case when qtile=1 then second_diff else null end) || ' seconds' ELSE '' END hijack_exception,
            sum(case when second_diff between  0 and   20 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 21 and   40 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 41 and   60 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 61 and   80 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 81 and  100 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 101 and 120 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 121 and 140 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 141 and 160 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 161 and 180 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 181 and 200 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 201 and 220 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 221 and 240 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 241 and 260 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 261 and 280 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 281 and 300 then 1 else 0 end )  Qty_Per_20_second,
           sum(case when second_diff between 0 and 300 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 301 and 600 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 601 and 900 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 901 and 1200then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 1201 and 1200 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 1501 and 1800 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 1801 and 2100 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 2101 and 2400 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 2401 and 2700 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 2701 and 3000 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 3001 and 3300 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 3301 and 3600 then 1 else 0 end ) Qty_Per_5_minute,
           sum(case when second_diff between     0 and  3600 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between  3601 and  7200 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between  7201 and 10800 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 10801 and 14400 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 14401 and 18000 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 18001 and 21600 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 21601 and 25200 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 25201 and 28800 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 28801 and 32400 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 32401 and 36000 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 36001 and 39600 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 39601 and 43200 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 43201 and 46800 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 46801 and 50400 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 50401 and 54000 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 54001 and 57600 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 57601 and 61200 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 61201 and 64800 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 64801 and 68400 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 68401 and 72000 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 72001 and 75600 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 75601 and 79200 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 79201 and 82800 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 82801 and 86400 then 1 else 0 end )  Qty_Per_hour,
    round(sum(case when device_type = 'aphone' then 1 else 0 end) * 100 /count(*)::decimal,1)  pct_aPhone,
    round(sum(case when device_type = 'iphone' then 1 else 0 end) * 100 /count(*)::decimal,1)  pct_iPhone,
    round(sum(case when device_type = 'atablet' then 1 else 0 end) * 100 /count(*)::decimal,1)  pct_aTab,
    round(sum(case when device_type = 'itablet' then 1 else 0 end) * 100 /count(*)::decimal,1)  pct_iTab,
    round( sum( case when match_type = 'fingerpr' then 1 else 0 end) * 100 / count(*) :: decimal,1) pct_fingerprint,
    round( sum( case when match_type = 'deviceid' then 1 else 0 end) * 100 / count(*) :: decimal,1) pct_deviceid,
    round( sum( case when match_type = 'referrer' then 1 else 0 end) * 100 / count(*) :: decimal,1) pct_referrer,
    round( sum( case when match_type = 'inline' then 1 else 0 end) * 100 / count(*) :: decimal,1) pct_inline,
    round( sum( case when match_type = 'preload' then 1 else 0 end) * 100 / count(*) :: decimal,1) pct_preload,
    round( sum( case when match_type not in ('fingerpr','deviceid','referrer','inline','preload') then 1 else 0 end) * 100 / count(*) :: decimal,1) pct_other,
    round( sum( case status_code when  0 then 1 else 0 end ) * 100 / count(*) :: decimal,1) pct_approved,
    round( sum( case status_code when 81 then 1 else 0 end ) * 100 / count(*) :: decimal,1) pct_app_30_day,
    round( sum( case status_code when 80 then 1 else 0 end ) * 100 / count(*) :: decimal,1) pct_app_reinstall,
    round( sum( case status_code when 50 then 1 else 0 end ) * 100 / count(*) :: decimal,1) pct_rej_jailbroke,
    round( sum( case status_code when 51 then 1 else 0 end ) * 100 / count(*) :: decimal,1) pct_rej_invalid,
    round( sum( case status_code when 72 then 1 else 0 end ) * 100 / count(*) :: decimal,1) pct_rej_unattr,
    round( sum( case status_code when 73 then 1 else 0 end ) * 100 / count(*) :: decimal,1) pct_rej_already
    FROM (
    SELECT date(date_trunc('week', log_date_hour)) traffic_week, advertiser_name, advertiser_id, site_name, site_id, publisher_name, publisher_id,
        second_diff, count(1) over(PARTITION by date(date_trunc('week', log_date_hour)), advertiser_name, site_name, publisher_name) pub_qty,
        publisher_sub_campaign,
        ntile(4) OVER (PARTITION by date(date_trunc('week', log_date_hour)), advertiser_name,site_name, publisher_name, publisher_sub_campaign ORDER BY second_diff ) qtile, device_type, match_type, log_status, status_code
        FROM tmc_fraud_lag_install_detailed
        WHERE log_Date_hour >= '2017-05-22' and log_date_hour < '2017-05-29'
            and attributable_type = 'click'
            and match_type <> 'click' AND second_diff <= 24*60*60 and installs > 0
            -- removing TUNE-generated clicks
            and
            (case when match_type = 'preload'
            or (match_type = 'inline' AND publisher_name like 'Facebook%')
            or (match_type = 'referrer' AND publisher_name IN('Facebook Organic', 'Google Organic'))
            then 1 else 0 end) = 0
--            and publisher_sub_campaign is not null and publisher_sub_campaign <> ''
        ) a
    where publisher_sub_campaign is not null and publisher_sub_campaign <> ''
    GROUP BY 1,2,3,4,5,6,7,9,10,11,12
    HAVING max(case when qtile=2 then second_diff else null end) - min(case when qtile=2 then second_diff else null end) >0
       AND max(case when qtile=3 then second_diff else null end) - min(case when qtile=3 then second_diff else null end) >0
       AND ( round( ( max(case when qtile=2 then second_diff else null end) - min(case when qtile=2 then second_diff else null end) ) /
                    ( max(case when qtile=3 then second_diff else null end) - min(case when qtile=3 then second_diff else null end) )  ::decimal,3)  > 0.9
       OR max(case when qtile=1 then second_diff else null end) <= 30 )
       AND count(*) > 200
UNION ALL
-- Add publisher_sub_publisher to group by ------------------------------------------------------------------------------
SELECT  advertiser_name, advertiser_id, site_name, site_id, publisher_name, publisher_id, traffic_week, max(pub_qty) pub_qty, '' publisher_sub_campaign, publisher_sub_publisher,'' country_name, '' Country_Sub_publisher,
  count(*) install_qty,
  round(count(*) * 100 / max(pub_qty) ::decimal,2) pct_of_traffic,
    max(case when qtile=1 then second_diff else null end) q1_time,
    max(case when qtile=2 then second_diff else null end) - min(case when qtile=2 then second_diff else null end) q2_time,
    max(case when qtile=3 then second_diff else null end) - min(case when qtile=3 then second_diff else null end) q3_time,
    max(case when qtile=4 then second_diff else null end) - min(case when qtile=4 then second_diff else null end) q4_time,
    round( ( max(case when qtile=2 then second_diff else null end) - min(case when qtile=2 then second_diff else null end) ) /
                    ( max(case when qtile=3 then second_diff else null end) - min(case when qtile=3 then second_diff else null end) )  ::decimal,3) ratio_Q2_over_Q3,
    case when round( ( max(case when qtile=2 then second_diff else null end) - min(case when qtile=2 then second_diff else null end) ) /
                    ( max(case when qtile=3 then second_diff else null end) - min(case when qtile=3 then second_diff else null end) )  ::decimal,3) > 0.9
         then 'linear trend appears' ELSE '' END linear_exception,
    case when max(case when qtile=1 then second_diff else null end) <= 30
         then '25% OF traffic WITHIN ' || max(case when qtile=1 then second_diff else null end) || ' seconds' ELSE '' END hijack_exception,
            sum(case when second_diff between  0 and   20 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 21 and   40 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 41 and   60 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 61 and   80 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 81 and  100 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 101 and 120 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 121 and 140 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 141 and 160 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 161 and 180 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 181 and 200 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 201 and 220 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 221 and 240 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 241 and 260 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 261 and 280 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 281 and 300 then 1 else 0 end )  Qty_Per_20_second,
           sum(case when second_diff between 0 and 300 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 301 and 600 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 601 and 900 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 901 and 1200then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 1201 and 1200 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 1501 and 1800 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 1801 and 2100 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 2101 and 2400 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 2401 and 2700 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 2701 and 3000 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 3001 and 3300 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 3301 and 3600 then 1 else 0 end ) Qty_Per_5_minute,
           sum(case when second_diff between     0 and  3600 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between  3601 and  7200 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between  7201 and 10800 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 10801 and 14400 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 14401 and 18000 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 18001 and 21600 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 21601 and 25200 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 25201 and 28800 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 28801 and 32400 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 32401 and 36000 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 36001 and 39600 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 39601 and 43200 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 43201 and 46800 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 46801 and 50400 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 50401 and 54000 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 54001 and 57600 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 57601 and 61200 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 61201 and 64800 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 64801 and 68400 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 68401 and 72000 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 72001 and 75600 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 75601 and 79200 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 79201 and 82800 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 82801 and 86400 then 1 else 0 end )  Qty_Per_hour,
    round(sum(case when device_type = 'aphone' then 1 else 0 end) * 100 /count(*)::decimal,1)  pct_aPhone,
    round(sum(case when device_type = 'iphone' then 1 else 0 end) * 100 /count(*)::decimal,1)  pct_iPhone,
    round(sum(case when device_type = 'atablet' then 1 else 0 end) * 100 /count(*)::decimal,1)  pct_aTab,
    round(sum(case when device_type = 'itablet' then 1 else 0 end) * 100 /count(*)::decimal,1)  pct_iTab,
    round( sum( case when match_type = 'fingerpr' then 1 else 0 end) * 100 / count(*) :: decimal,1) pct_fingerprint,
    round( sum( case when match_type = 'deviceid' then 1 else 0 end) * 100 / count(*) :: decimal,1) pct_deviceid,
    round( sum( case when match_type = 'referrer' then 1 else 0 end) * 100 / count(*) :: decimal,1) pct_referrer,
    round( sum( case when match_type = 'inline' then 1 else 0 end) * 100 / count(*) :: decimal,1) pct_inline,
    round( sum( case when match_type = 'preload' then 1 else 0 end) * 100 / count(*) :: decimal,1) pct_preload,
    round( sum( case when match_type not in ('fingerpr','deviceid','referrer','inline','preload') then 1 else 0 end) * 100 / count(*) :: decimal,1) pct_other,
    round( sum( case status_code when  0 then 1 else 0 end ) * 100 / count(*) :: decimal,1) pct_approved,
    round( sum( case status_code when 81 then 1 else 0 end ) * 100 / count(*) :: decimal,1) pct_app_30_day,
    round( sum( case status_code when 80 then 1 else 0 end ) * 100 / count(*) :: decimal,1) pct_app_reinstall,
    round( sum( case status_code when 50 then 1 else 0 end ) * 100 / count(*) :: decimal,1) pct_rej_jailbroke,
    round( sum( case status_code when 51 then 1 else 0 end ) * 100 / count(*) :: decimal,1) pct_rej_invalid,
    round( sum( case status_code when 72 then 1 else 0 end ) * 100 / count(*) :: decimal,1) pct_rej_unattr,
    round( sum( case status_code when 73 then 1 else 0 end ) * 100 / count(*) :: decimal,1) pct_rej_already
    FROM (
    SELECT date(date_trunc('week', log_date_hour)) traffic_week, advertiser_name, advertiser_id, site_name, site_id, publisher_name, publisher_id,
        second_diff, count(1) over(PARTITION by date(date_trunc('week', log_date_hour)), advertiser_name, site_name, publisher_name) pub_qty,
        publisher_sub_publisher,
        ntile(4) OVER (PARTITION by date(date_trunc('week', log_date_hour)), advertiser_name,site_name, publisher_name, publisher_sub_publisher ORDER BY second_diff ) qtile, device_type, match_type, log_status, status_code
        FROM tmc_fraud_lag_install_detailed
        WHERE log_Date_hour >= '2017-05-22' and log_date_hour < '2017-05-29'
            and attributable_type = 'click'
            and match_type <> 'click' AND second_diff <= 24*60*60  and installs > 0
            -- removing TUNE-generated clicks
            and
            (case when match_type = 'preload'
            or (match_type = 'inline' AND publisher_name like 'Facebook%')
            or (match_type = 'referrer' AND publisher_name IN('Facebook Organic', 'Google Organic'))
            then 1 else 0 end) = 0
--         and publisher_sub_publisher is not null and publisher_sub_publisher <> ''
        ) a
    where publisher_sub_publisher is not null and publisher_sub_publisher <> ''
    GROUP BY 1,2,3,4,5,6,7,9,10,11,12
    HAVING max(case when qtile=2 then second_diff else null end) - min(case when qtile=2 then second_diff else null end) >0
       AND max(case when qtile=3 then second_diff else null end) - min(case when qtile=3 then second_diff else null end) >0
       AND ( round( ( max(case when qtile=2 then second_diff else null end) - min(case when qtile=2 then second_diff else null end) ) /
                    ( max(case when qtile=3 then second_diff else null end) - min(case when qtile=3 then second_diff else null end) )  ::decimal,3)  > 0.9
       OR max(case when qtile=1 then second_diff else null end) <= 30 )
       AND count(*) > 200
UNION ALL
-- Add country_name to group by ------------------------------------------------------------------------------
SELECT  advertiser_name, advertiser_id, site_name, site_id, publisher_name, publisher_id, traffic_week, max(pub_qty) pub_qty, '' publisher_sub_campaign, '' publisher_sub_publisher, country_name, '' Country_Sub_publisher,
  count(*) install_qty,
  round(count(*) * 100 / max(pub_qty) ::decimal,2) pct_of_traffic,
    max(case when qtile=1 then second_diff else null end) q1_time,
    max(case when qtile=2 then second_diff else null end) - min(case when qtile=2 then second_diff else null end) q2_time,
    max(case when qtile=3 then second_diff else null end) - min(case when qtile=3 then second_diff else null end) q3_time,
    max(case when qtile=4 then second_diff else null end) - min(case when qtile=4 then second_diff else null end) q4_time,
    round( ( max(case when qtile=2 then second_diff else null end) - min(case when qtile=2 then second_diff else null end) ) /
                    ( max(case when qtile=3 then second_diff else null end) - min(case when qtile=3 then second_diff else null end) )  ::decimal,3) ratio_Q2_over_Q3,
    case when round( ( max(case when qtile=2 then second_diff else null end) - min(case when qtile=2 then second_diff else null end) ) /
                    ( max(case when qtile=3 then second_diff else null end) - min(case when qtile=3 then second_diff else null end) )  ::decimal,3) > 0.9
         then 'linear trend appears' ELSE '' END linear_exception,
    case when max(case when qtile=1 then second_diff else null end) <= 30
         then '25% OF traffic WITHIN ' || max(case when qtile=1 then second_diff else null end) || ' seconds' ELSE '' END hijack_exception,
            sum(case when second_diff between  0 and   20 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 21 and   40 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 41 and   60 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 61 and   80 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 81 and  100 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 101 and 120 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 121 and 140 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 141 and 160 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 161 and 180 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 181 and 200 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 201 and 220 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 221 and 240 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 241 and 260 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 261 and 280 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 281 and 300 then 1 else 0 end )  Qty_Per_20_second,
           sum(case when second_diff between 0 and 300 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 301 and 600 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 601 and 900 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 901 and 1200then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 1201 and 1200 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 1501 and 1800 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 1801 and 2100 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 2101 and 2400 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 2401 and 2700 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 2701 and 3000 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 3001 and 3300 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 3301 and 3600 then 1 else 0 end ) Qty_Per_5_minute,
           sum(case when second_diff between     0 and  3600 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between  3601 and  7200 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between  7201 and 10800 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 10801 and 14400 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 14401 and 18000 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 18001 and 21600 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 21601 and 25200 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 25201 and 28800 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 28801 and 32400 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 32401 and 36000 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 36001 and 39600 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 39601 and 43200 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 43201 and 46800 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 46801 and 50400 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 50401 and 54000 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 54001 and 57600 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 57601 and 61200 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 61201 and 64800 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 64801 and 68400 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 68401 and 72000 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 72001 and 75600 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 75601 and 79200 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 79201 and 82800 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 82801 and 86400 then 1 else 0 end )  Qty_Per_hour,
    round(sum(case when device_type = 'aphone' then 1 else 0 end) * 100 /count(*)::decimal,1)  pct_aPhone,
    round(sum(case when device_type = 'iphone' then 1 else 0 end) * 100 /count(*)::decimal,1)  pct_iPhone,
    round(sum(case when device_type = 'atablet' then 1 else 0 end) * 100 /count(*)::decimal,1)  pct_aTab,
    round(sum(case when device_type = 'itablet' then 1 else 0 end) * 100 /count(*)::decimal,1)  pct_iTab,
    round( sum( case when match_type = 'fingerpr' then 1 else 0 end) * 100 / count(*) :: decimal,1) pct_fingerprint,
    round( sum( case when match_type = 'deviceid' then 1 else 0 end) * 100 / count(*) :: decimal,1) pct_deviceid,
    round( sum( case when match_type = 'referrer' then 1 else 0 end) * 100 / count(*) :: decimal,1) pct_referrer,
    round( sum( case when match_type = 'inline' then 1 else 0 end) * 100 / count(*) :: decimal,1) pct_inline,
    round( sum( case when match_type = 'preload' then 1 else 0 end) * 100 / count(*) :: decimal,1) pct_preload,
    round( sum( case when match_type not in ('fingerpr','deviceid','referrer','inline','preload') then 1 else 0 end) * 100 / count(*) :: decimal,1) pct_other,
    round( sum( case status_code when  0 then 1 else 0 end ) * 100 / count(*) :: decimal,1) pct_approved,
    round( sum( case status_code when 81 then 1 else 0 end ) * 100 / count(*) :: decimal,1) pct_app_30_day,
    round( sum( case status_code when 80 then 1 else 0 end ) * 100 / count(*) :: decimal,1) pct_app_reinstall,
    round( sum( case status_code when 50 then 1 else 0 end ) * 100 / count(*) :: decimal,1) pct_rej_jailbroke,
    round( sum( case status_code when 51 then 1 else 0 end ) * 100 / count(*) :: decimal,1) pct_rej_invalid,
    round( sum( case status_code when 72 then 1 else 0 end ) * 100 / count(*) :: decimal,1) pct_rej_unattr,
    round( sum( case status_code when 73 then 1 else 0 end ) * 100 / count(*) :: decimal,1) pct_rej_already
    FROM (
    SELECT date(date_trunc('week', log_date_hour)) traffic_week, advertiser_name, advertiser_id, site_name, site_id, publisher_name, publisher_id,
        second_diff, count(1) over(PARTITION by date(date_trunc('week', log_date_hour)), advertiser_name, site_name, publisher_name) pub_qty,
        country_name,
        ntile(4) OVER (PARTITION by date(date_trunc('week', log_date_hour)), advertiser_name,site_name, publisher_name, country_name ORDER BY second_diff ) qtile, device_type, match_type, log_status, status_code
        FROM tmc_fraud_lag_install_detailed
        WHERE log_Date_hour >= '2017-05-22' and log_date_hour < '2017-05-29'
            and attributable_type = 'click'
            and match_type <> 'click' AND second_diff <= 24*60*60  and installs > 0
            -- removing TUNE-generated clicks
            and
            (case when match_type = 'preload'
            or (match_type = 'inline' AND publisher_name like 'Facebook%')
            or (match_type = 'referrer' AND publisher_name IN('Facebook Organic', 'Google Organic'))
            then 1 else 0 end) = 0
--             and country_name is not null and country_name <> ''
        ) a
    where country_name is not null and country_name <> ''
    GROUP BY 1,2,3,4,5,6,7,9,10,11,12
    HAVING max(case when qtile=2 then second_diff else null end) - min(case when qtile=2 then second_diff else null end) >0
       AND max(case when qtile=3 then second_diff else null end) - min(case when qtile=3 then second_diff else null end) >0
       AND ( round( ( max(case when qtile=2 then second_diff else null end) - min(case when qtile=2 then second_diff else null end) ) /
                    ( max(case when qtile=3 then second_diff else null end) - min(case when qtile=3 then second_diff else null end) )  ::decimal,3)  > 0.9
       OR max(case when qtile=1 then second_diff else null end) <= 30 )
       AND count(*) > 200
UNION ALL
-- Add country_name ++ publisher_sub_publisher to group by ------------------------------------------------------------------------------
SELECT  advertiser_name, advertiser_id, site_name, site_id, publisher_name, publisher_id, traffic_week, max(pub_qty) pub_qty, '' publisher_sub_campaign, '' publisher_sub_publisher, '' country_name,
  case when length(country_name) > 0 and length(publisher_sub_publisher) >0 then country_name || '+'||  publisher_sub_publisher else '' end Country_Sub_publisher,
  count(*) install_qty,
   round(count(*) * 100 / max(pub_qty) ::decimal,2) pct_of_traffic,
    max(case when qtile=1 then second_diff else null end) q1_time,
    max(case when qtile=2 then second_diff else null end) - min(case when qtile=2 then second_diff else null end) q2_time,
    max(case when qtile=3 then second_diff else null end) - min(case when qtile=3 then second_diff else null end) q3_time,
    max(case when qtile=4 then second_diff else null end) - min(case when qtile=4 then second_diff else null end) q4_time,
    round( ( max(case when qtile=2 then second_diff else null end) - min(case when qtile=2 then second_diff else null end) ) /
                    ( max(case when qtile=3 then second_diff else null end) - min(case when qtile=3 then second_diff else null end) )  ::decimal,3) ratio_Q2_over_Q3,
    case when round( ( max(case when qtile=2 then second_diff else null end) - min(case when qtile=2 then second_diff else null end) ) /
                    ( max(case when qtile=3 then second_diff else null end) - min(case when qtile=3 then second_diff else null end) )  ::decimal,3) > 0.9
         then 'linear trend appears' ELSE '' END linear_exception,
    case when max(case when qtile=1 then second_diff else null end) <= 30
         then '25% OF traffic WITHIN ' || max(case when qtile=1 then second_diff else null end) || ' seconds' ELSE '' END hijack_exception,
            sum(case when second_diff between  0 and   20 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 21 and   40 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 41 and   60 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 61 and   80 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 81 and  100 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 101 and 120 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 121 and 140 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 141 and 160 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 161 and 180 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 181 and 200 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 201 and 220 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 221 and 240 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 241 and 260 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 261 and 280 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 281 and 300 then 1 else 0 end )  Qty_Per_20_second,
           sum(case when second_diff between 0 and 300 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 301 and 600 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 601 and 900 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 901 and 1200then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 1201 and 1200 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 1501 and 1800 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 1801 and 2100 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 2101 and 2400 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 2401 and 2700 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 2701 and 3000 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 3001 and 3300 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 3301 and 3600 then 1 else 0 end ) Qty_Per_5_minute,
           sum(case when second_diff between     0 and  3600 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between  3601 and  7200 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between  7201 and 10800 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 10801 and 14400 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 14401 and 18000 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 18001 and 21600 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 21601 and 25200 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 25201 and 28800 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 28801 and 32400 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 32401 and 36000 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 36001 and 39600 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 39601 and 43200 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 43201 and 46800 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 46801 and 50400 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 50401 and 54000 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 54001 and 57600 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 57601 and 61200 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 61201 and 64800 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 64801 and 68400 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 68401 and 72000 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 72001 and 75600 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 75601 and 79200 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 79201 and 82800 then 1 else 0 end )
    || ',' ||  sum(case when second_diff between 82801 and 86400 then 1 else 0 end )  Qty_Per_hour,
    round(sum(case when device_type = 'aphone' then 1 else 0 end) * 100 /count(*)::decimal,1)  pct_aPhone,
    round(sum(case when device_type = 'iphone' then 1 else 0 end) * 100 /count(*)::decimal,1)  pct_iPhone,
    round(sum(case when device_type = 'atablet' then 1 else 0 end) * 100 /count(*)::decimal,1)  pct_aTab,
    round(sum(case when device_type = 'itablet' then 1 else 0 end) * 100 /count(*)::decimal,1)  pct_iTab,
    round( sum( case when match_type = 'fingerpr' then 1 else 0 end) * 100 / count(*) :: decimal,1) pct_fingerprint,
    round( sum( case when match_type = 'deviceid' then 1 else 0 end) * 100 / count(*) :: decimal,1) pct_deviceid,
    round( sum( case when match_type = 'referrer' then 1 else 0 end) * 100 / count(*) :: decimal,1) pct_referrer,
    round( sum( case when match_type = 'inline' then 1 else 0 end) * 100 / count(*) :: decimal,1) pct_inline,
    round( sum( case when match_type = 'preload' then 1 else 0 end) * 100 / count(*) :: decimal,1) pct_preload,
    round( sum( case when match_type not in ('fingerpr','deviceid','referrer','inline','preload') then 1 else 0 end) * 100 / count(*) :: decimal,1) pct_other,
    round( sum( case status_code when  0 then 1 else 0 end ) * 100 / count(*) :: decimal,1) pct_approved,
    round( sum( case status_code when 81 then 1 else 0 end ) * 100 / count(*) :: decimal,1) pct_app_30_day,
    round( sum( case status_code when 80 then 1 else 0 end ) * 100 / count(*) :: decimal,1) pct_app_reinstall,
    round( sum( case status_code when 50 then 1 else 0 end ) * 100 / count(*) :: decimal,1) pct_rej_jailbroke,
    round( sum( case status_code when 51 then 1 else 0 end ) * 100 / count(*) :: decimal,1) pct_rej_invalid,
    round( sum( case status_code when 72 then 1 else 0 end ) * 100 / count(*) :: decimal,1) pct_rej_unattr,
    round( sum( case status_code when 73 then 1 else 0 end ) * 100 / count(*) :: decimal,1) pct_rej_already
    FROM (
    SELECT date(date_trunc('week', log_date_hour)) traffic_week, advertiser_name, advertiser_id, site_name, site_id, publisher_name, publisher_id,
        second_diff, count(1) over(PARTITION by date(date_trunc('week', log_date_hour)), advertiser_name, site_name, publisher_name) pub_qty,
        country_name, publisher_sub_publisher,
        ntile(4) OVER (PARTITION by date(date_trunc('week', log_date_hour)), advertiser_name,site_name, publisher_name, country_name , publisher_sub_publisher ORDER BY second_diff ) qtile, device_type, match_type, log_status, status_code
        FROM tmc_fraud_lag_install_detailed
        WHERE log_Date_hour >= '2017-05-22' and log_date_hour < '2017-05-29' and installs > 0
            and attributable_type = 'click'
            and match_type <> 'click' AND second_diff <= 24*60*60
            -- removing TUNE-generated clicks
            and
            (case when match_type = 'preload'
            or (match_type = 'inline' AND publisher_name like 'Facebook%')
            or (match_type = 'referrer' AND publisher_name IN('Facebook Organic', 'Google Organic'))
            then 1 else 0 end) = 0            
 --           and publisher_sub_publisher is not null and publisher_sub_publisher <> ''    and country_name is not null and country_name <> ''
        ) a
    where publisher_sub_publisher is not null and publisher_sub_publisher <> ''    and country_name is not null and country_name <> ''
    GROUP BY 1,2,3,4,5,6,7,9,10,11,12
    HAVING max(case when qtile=2 then second_diff else null end) - min(case when qtile=2 then second_diff else null end) >0
       AND max(case when qtile=3 then second_diff else null end) - min(case when qtile=3 then second_diff else null end) >0
       AND ( round( ( max(case when qtile=2 then second_diff else null end) - min(case when qtile=2 then second_diff else null end) ) /
                    ( max(case when qtile=3 then second_diff else null end) - min(case when qtile=3 then second_diff else null end) )  ::decimal,3)  > 0.9
       OR max(case when qtile=1 then second_diff else null end) <= 30 )
       AND count(*) > 200
order by 1,2,3,4,5,6,7,8,9,10,11;
commit;




select  traffic_week,   count(*)
from tmp_Fraud_weekly_Recommendation where traffic_week >= '2017-01-01' group by 1
order by 1;
