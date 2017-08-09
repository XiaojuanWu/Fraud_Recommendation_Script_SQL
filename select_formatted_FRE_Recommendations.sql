-- ------------------------------------------------------------
--  linear_exception ------------------------------------------
-- ------------------------------------------------------------

select  advertiser_name, publisher_name, site_name, traffic_week, max(adv_site_pub_wk_qty) adv_site_pub_wk_traffic_qty,
   greatest(
  sum(case when length(country_name) = 0 and length(publisher_sub_publisher) = 0 and length(country_sub_publisher) = 0 and length(publisher_sub_campaign) = 0 then subset_qty else 0 end ) ,
  sum(case when length(country_name) > 0 and length(publisher_sub_publisher) = 0 then subset_qty else 0 end ) ,
  sum(case when length(publisher_sub_campaign) > 0 then subset_qty else 0 end ) ,
  sum(case when length(country_name) = 0 and length(publisher_sub_publisher) > 0 then subset_qty else 0 end ) ,
  sum(case when length(country_sub_publisher) > 0 then subset_qty else 0 end )
    )  subset_qty,
  listagg(country_name,',' ) Country_list,
  'Linear' AS Fraud_Pattern,
  'Recommend: ' ||
  CASE
      WHEN max(LOWER(publisher_name)) = 'youtube'   THEN 'MONITOR '
      WHEN greatest(
      sum(case when length(country_name) = 0 and length(publisher_sub_publisher) = 0 and length(country_sub_publisher) = 0 and length(publisher_sub_campaign) = 0 then subset_qty else 0 end ) ,
      sum(case when length(country_name) > 0 and length(publisher_sub_publisher) = 0 then subset_qty else 0 end ) ,
      sum(case when length(publisher_sub_campaign) > 0 then subset_qty else 0 end ) ,
      sum(case when length(country_name) = 0 and length(publisher_sub_publisher) > 0 then subset_qty else 0 end ) ,
      sum(case when length(country_sub_publisher) > 0 then subset_qty else 0 end )
        )*1.0/max(adv_site_pub_wk_qty) >= .35 then 'REVIEW ALL '
      WHEN greatest(
      sum(case when length(country_name) = 0 and length(publisher_sub_publisher) = 0 and length(country_sub_publisher) = 0 and length(publisher_sub_campaign) = 0 then subset_qty else 0 end ) ,
      sum(case when length(country_name) > 0 and length(publisher_sub_publisher) = 0 then subset_qty else 0 end ) ,
      sum(case when length(publisher_sub_campaign) > 0 then subset_qty else 0 end ) ,
      sum(case when length(country_name) = 0 and length(publisher_sub_publisher) > 0 then subset_qty else 0 end ) ,
      sum(case when length(country_sub_publisher) > 0 then subset_qty else 0 end )
        )*1.0/max(adv_site_pub_wk_qty) > .05 then 'REVIEW SUBSET(S) of '
  ELSE 'MONITOR ' END
                          || 'traffic from Partner: ' || publisher_name || ' for site: ' || site_name || ' week starting ' || traffic_week ||
    '. \r\nThere were ' || max(adv_site_pub_wk_qty) || ' installs this week, with ' ||
     greatest(
  sum(case when length(country_name) = 0 and length(publisher_sub_publisher) = 0 and length(country_sub_publisher) = 0 and length(publisher_sub_campaign) = 0 then subset_qty else 0 end ) ,
  sum(case when length(country_name) > 0 and length(publisher_sub_publisher) = 0 then subset_qty else 0 end ) ,
  sum(case when length(publisher_sub_campaign) > 0 then subset_qty else 0 end ) ,
  sum(case when length(country_name) = 0 and length(publisher_sub_publisher) > 0 then subset_qty else 0 end ) ,
  sum(case when length(country_sub_publisher) > 0 then subset_qty else 0 end )
    )  || ' installs having the following characteristics: ' || '\r\n' ||
          case when length(max(linear_exception)) >0 then  '-- Linear pattern indicating artificial generation of clicks ------------------------------------' || '\r\n'  else '' end ||
          --case when min(q1_time) <= 30 then '-- Click Insertion pattern (installs very close to clicks) with 25% of opens occurring under 30 seconds' || '\r\n'  else '' end  ||
--          case when max(pct_app_reinstall) >= 30  then '-- Approved Re-Installs occurs at ' || max(pct_app_reinstall) || '% of the time\r\n'  else '' end ||
          case when max(pct_rej_jailbroke) >= 2  then '-- Rejected with OS Jailbroken occurs ' || max(pct_rej_jailbroke) || '% of the time\r\n'  else '' end ||
          case when max(pct_rej_invalid) >= 2  then '-- Rejected - Invalid occurs ' || max(pct_rej_invalid) || '% of the time\r\n'  else '' end ||
          case when max(pct_rej_unattr) >= 2  then '-- Rejected - Unattributable occurs ' || max(pct_rej_unattr) || '% of the time\r\n'  else '' end ||
          case when max(pct_rej_already) >= 2  then '-- Rejected - Previously Installed occurs ' || max(pct_rej_already) || '% of the time\r\n'  else '' end ||
    'The pattern appears in the following traffic groups: ' ||
    '\r\n' ||
    case when length(listagg(publisher_sub_campaign,',' ) || listagg(publisher_sub_publisher,',' ) || listagg(country_name,',') || listagg(country_sub_publisher,',') ) < 1
      then '     All traffic for this Partner.' || '\r\n'
      ELSE
        case when length(listagg(country_name,',' )) > 0 then '     Countries: ' || listagg(case country_name when '' then '' else country_name || '( qty-' || subset_qty || ' ) ' end ,',' )  within group ( order by subset_qty desc ) || '\r\n' else '' end  ||
        case when length(listagg(publisher_sub_campaign ,',' )) > 0 then '     Sub-Campaign(s): ' || listagg(case publisher_sub_campaign when '' then '' else publisher_sub_campaign || '( qty-' || subset_qty || ' ) ' end ,',' ) within group ( order by subset_qty desc ) || '\r\n' else '' end  ||
        case when length(listagg(publisher_sub_publisher,',' )) > 0 then '     Sub-Partner(s): ' || listagg(case publisher_sub_publisher when '' then '' else publisher_sub_publisher || '( qty-' || subset_qty || ' ) ' end ,',' ) within group ( order by subset_qty desc ) || '\r\n' else '' end ||
        case when length(listagg(country_sub_publisher,',' )) > 0 then '     Country+Sub-Partner(s): ' || listagg(case country_sub_publisher when '' then '' else country_sub_publisher || '( qty-' || subset_qty || ' ) ' end ,',' ) within group ( order by subset_qty desc ) || '\r\n' else '' end
    end
    ||   'The most frequent attribution match types were: '
    ||  case when sum(pct_fingerprint * subset_qty) / sum(subset_qty) > 10 then 'Fingerprint-' ||  round(sum(pct_fingerprint * subset_qty)/sum(subset_qty),2)  || '% ' ELSE ' ' end
    ||  case when sum(pct_deviceid * subset_qty) / sum(subset_qty) > 10 then 'Device Id-' ||  round(sum(pct_deviceid * subset_qty)/sum(subset_qty),2)  || '% ' ELSE ' ' end
    ||  case when sum(pct_referrer * subset_qty) / sum(subset_qty) > 10 then 'Referrer-' ||  round(sum(pct_referrer * subset_qty)/sum(subset_qty),2)  || '% ' ELSE ' ' end
    ||  case when sum(pct_inline * subset_qty) / sum(subset_qty) > 10 then 'Inline-' ||  round(sum(pct_inline * subset_qty)/sum(subset_qty),2)  || '% ' ELSE ' ' end
    ||  case when sum(pct_preload * subset_qty) / sum(subset_qty) > 10 then 'Preload-' ||  round(sum(pct_preload * subset_qty)/sum(subset_qty),2)  || '% ' ELSE ' ' end
    || '\r\n' ||   'The rejection status codes were: '
    ||  case when sum(pct_approved) >  0 then ' Approved:' ||  round(sum(pct_approved * subset_qty)/sum(subset_qty),2)  || '% ' ELSE ' ' end
    ||  case when sum(pct_app_30_day) >  0 then ' Approved 30+ Days Receipt:' ||  round(sum(pct_app_30_day * subset_qty)/sum(subset_qty),2)  || '% ' ELSE ' ' end
    ||  case when sum(pct_app_reinstall) >  0 then ' Approved Re-Install:' ||  round(sum(pct_app_reinstall * subset_qty)/sum(subset_qty),2)  || '% ' ELSE ' ' end
    ||  case when sum(pct_rej_jailbroke) >  0 then ' Rejected Jailbroken OS:' ||  round(sum(pct_rej_jailbroke * subset_qty)/sum(subset_qty),2)  || '% ' ELSE ' ' end
    ||  case when sum(pct_rej_invalid) >  0 then ' Rejected Invalid Receipt:' ||  round(sum(pct_rej_invalid * subset_qty)/sum(subset_qty),2)  || '% ' ELSE ' ' end
    ||  case when sum(pct_rej_unattr) >  0 then ' Post Conversion Unattributable:' ||  round(sum(pct_rej_unattr * subset_qty)/sum(subset_qty),2)  || '% ' ELSE ' ' end
    ||  case when sum(pct_rej_already) >  0 then ' Post Conversion Already Attributed:' ||  round(sum(pct_rej_already * subset_qty)/sum(subset_qty),2)  || '% ' ELSE ' ' end
  Recommendation
from tmp_fraud_weekly_recommendation
 where
    traffic_week >= '2017-05-01' and publisher_name = 'InMobi'
  and (length(linear_exception) > 0 )
group by advertiser_name, publisher_name, site_name, traffic_week
union all
select  advertiser_name, publisher_name, site_name, traffic_week, max(adv_site_pub_wk_qty) adv_site_pub_wk_traffic_qty,
   greatest(
  sum(case when length(country_name) = 0 and length(publisher_sub_publisher) = 0 and length(country_sub_publisher) = 0 and length(publisher_sub_campaign) = 0 then subset_qty else 0 end ) ,
  sum(case when length(country_name) > 0 and length(publisher_sub_publisher) = 0 then subset_qty else 0 end ) ,
  sum(case when length(publisher_sub_campaign) > 0 then subset_qty else 0 end ) ,
  sum(case when length(country_name) = 0 and length(publisher_sub_publisher) > 0 then subset_qty else 0 end ) ,
  sum(case when length(country_sub_publisher) > 0 then subset_qty else 0 end )
    )  subset_qty,
  listagg(country_name,',' ) Country_list,
  'Click Injection' AS Fraud_Pattern,
  'Recommend: ' ||
  CASE
      WHEN max(LOWER(publisher_name)) = 'youtube'   THEN 'MONITOR '
      WHEN greatest(
      sum(case when length(country_name) = 0 and length(publisher_sub_publisher) = 0 and length(country_sub_publisher) = 0 and length(publisher_sub_campaign) = 0 then subset_qty else 0 end ) ,
      sum(case when length(country_name) > 0 and length(publisher_sub_publisher) = 0 then subset_qty else 0 end ) ,
      sum(case when length(publisher_sub_campaign) > 0 then subset_qty else 0 end ) ,
      sum(case when length(country_name) = 0 and length(publisher_sub_publisher) > 0 then subset_qty else 0 end ) ,
      sum(case when length(country_sub_publisher) > 0 then subset_qty else 0 end )
        )*1.0/max(adv_site_pub_wk_qty) >= .35 then 'REVIEW ALL '
      WHEN greatest(
      sum(case when length(country_name) = 0 and length(publisher_sub_publisher) = 0 and length(country_sub_publisher) = 0 and length(publisher_sub_campaign) = 0 then subset_qty else 0 end ) ,
      sum(case when length(country_name) > 0 and length(publisher_sub_publisher) = 0 then subset_qty else 0 end ) ,
      sum(case when length(publisher_sub_campaign) > 0 then subset_qty else 0 end ) ,
      sum(case when length(country_name) = 0 and length(publisher_sub_publisher) > 0 then subset_qty else 0 end ) ,
      sum(case when length(country_sub_publisher) > 0 then subset_qty else 0 end )
        )*1.0/max(adv_site_pub_wk_qty) > .05 then 'REVIEW SUBSET(S) of '
  ELSE 'MONITOR ' END
                          || 'traffic from Partner: ' || publisher_name || ' for site: ' || site_name || ' for week starting ' || traffic_week ||
    '. \r\nThere were ' || max(adv_site_pub_wk_qty) || ' installs this week, with '  ||
     greatest(
  sum(case when length(country_name) = 0 and length(publisher_sub_publisher) = 0 and length(country_sub_publisher) = 0 and length(publisher_sub_campaign) = 0 then subset_qty else 0 end ) ,
  sum(case when length(country_name) > 0 and length(publisher_sub_publisher) = 0 then subset_qty else 0 end ) ,
  sum(case when length(publisher_sub_campaign) > 0 then subset_qty else 0 end ) ,
  sum(case when length(country_name) = 0 and length(publisher_sub_publisher) > 0 then subset_qty else 0 end ) ,
  sum(case when length(country_sub_publisher) > 0 then subset_qty else 0 end )
    )  || ' having the following characteristics: ' || '\r\n' ||
        --case when length(max(linear_exception)) >0 then  '-- Linear pattern indicating artificial generation of clicks ------------------------------------' || '\r\n'  else '' end ||
          case when min(q1_time) <= 30 then '-- Click Insertion pattern (installs very close to clicks) with 25% of opens occurring under 30 seconds' || '\r\n'  else '' end  ||
--          case when max(pct_app_reinstall) >= 30  then '-- Approved Re-Installs occurs at ' || max(pct_app_reinstall) || '% of the time\r\n'  else '' end ||
          case when max(pct_rej_jailbroke) >= 2  then '-- Rejected with OS Jailbroken occurs ' || max(pct_rej_jailbroke) || '% of the time\r\n'  else '' end ||
          case when max(pct_rej_invalid) >= 2  then '-- Rejected - Invalid occurs ' || max(pct_rej_invalid) || '% of the time\r\n'  else '' end ||
          case when max(pct_rej_unattr) >= 2  then '-- Rejected - Unattributable occurs ' || max(pct_rej_unattr) || '% of the time\r\n'  else '' end ||
          case when max(pct_rej_already) >= 2  then '-- Rejected - Previously Installed occurs ' || max(pct_rej_already) || '% of the time\r\n'  else '' end ||
    'The pattern appears in the following traffic groups: ' ||
    '\r\n' ||
    case when length(listagg(publisher_sub_campaign,',' ) || listagg(publisher_sub_publisher,',' ) || listagg(country_name,',') || listagg(country_sub_publisher,',') ) < 1
      then '     All traffic for this Partner.' || '\r\n'
      ELSE
        case when length(listagg(country_name,',' )) > 0 then '     Countries: ' || listagg(case country_name when '' then '' else country_name || '( qty-' || subset_qty || ' ) ' end ,',' )  within group ( order by subset_qty desc ) || '\r\n' else '' end  ||
        case when length(listagg(publisher_sub_campaign ,',' )) > 0 then '     Sub-Campaign(s): ' || listagg(case publisher_sub_campaign when '' then '' else publisher_sub_campaign || '( qty-' || subset_qty || ' ) ' end ,',' ) within group ( order by subset_qty desc ) || '\r\n' else '' end  ||
        case when length(listagg(publisher_sub_publisher,',' )) > 0 then '     Sub-Partner(s): ' || listagg(case publisher_sub_publisher when '' then '' else publisher_sub_publisher || '( qty-' || subset_qty || ' ) ' end ,',' ) within group ( order by subset_qty desc ) || '\r\n' else '' end ||
        case when length(listagg(country_sub_publisher,',' )) > 0 then '     Country+Sub-Partner(s): ' || listagg(case country_sub_publisher when '' then '' else country_sub_publisher || '( qty-' || subset_qty || ' ) ' end ,',' ) within group ( order by subset_qty desc ) || '\r\n' else '' end
    end
    ||   'The most frequent attribution match types were: '
    ||  case when sum(pct_fingerprint * subset_qty) / sum(subset_qty) > 10 then 'Fingerprint-' ||  round(sum(pct_fingerprint * subset_qty)/sum(subset_qty),2)  || '% ' ELSE ' ' end
    ||  case when sum(pct_deviceid * subset_qty) / sum(subset_qty) > 10 then 'Device Id-' ||  round(sum(pct_deviceid * subset_qty)/sum(subset_qty),2)  || '% ' ELSE ' ' end
    ||  case when sum(pct_referrer * subset_qty) / sum(subset_qty) > 10 then 'Referrer-' ||  round(sum(pct_referrer * subset_qty)/sum(subset_qty),2)  || '% ' ELSE ' ' end
    ||  case when sum(pct_inline * subset_qty) / sum(subset_qty) > 10 then 'Inline-' ||  round(sum(pct_inline * subset_qty)/sum(subset_qty),2)  || '% ' ELSE ' ' end
    ||  case when sum(pct_preload * subset_qty) / sum(subset_qty) > 10 then 'Preload-' ||  round(sum(pct_preload * subset_qty)/sum(subset_qty),2)  || '% ' ELSE ' ' end
    || '\r\n' ||   'The rejection status codes were: '
    ||  case when sum(pct_approved) >  0 then ' Approved:' ||  round(sum(pct_approved * subset_qty)/sum(subset_qty),2)  || '% ' ELSE ' ' end
    ||  case when sum(pct_app_30_day) >  0 then ' Approved 30+ Days Receipt:' ||  round(sum(pct_app_30_day * subset_qty)/sum(subset_qty),2)  || '% ' ELSE ' ' end
    ||  case when sum(pct_app_reinstall) >  0 then ' Approved Re-Install:' ||  round(sum(pct_app_reinstall * subset_qty)/sum(subset_qty),2)  || '% ' ELSE ' ' end
    ||  case when sum(pct_rej_jailbroke) >  0 then ' Rejected Jailbroken OS:' ||  round(sum(pct_rej_jailbroke * subset_qty)/sum(subset_qty),2)  || '% ' ELSE ' ' end
    ||  case when sum(pct_rej_invalid) >  0 then ' Rejected Invalid Receipt:' ||  round(sum(pct_rej_invalid * subset_qty)/sum(subset_qty),2)  || '% ' ELSE ' ' end
    ||  case when sum(pct_rej_unattr) >  0 then ' Post Conversion Unattributable:' ||  round(sum(pct_rej_unattr * subset_qty)/sum(subset_qty),2)  || '% ' ELSE ' ' end
    ||  case when sum(pct_rej_already) >  0 then ' Post Conversion Already Attributed:' ||  round(sum(pct_rej_already * subset_qty)/sum(subset_qty),2)  || '% ' ELSE ' ' end
  Recommendation
from tmp_fraud_weekly_recommendation
 where  traffic_week >= '2017-05-01' and publisher_name = 'InMobi'
  and (length(hijack_exception) > 0 )
group by advertiser_name, publisher_name, site_name, traffic_week
order by advertiser_name, publisher_name, site_name, traffic_week;


select * from tmp_fraud_weekly_recommendation
where    traffic_week >= '2017-05-01' and publisher_name = 'InMobi'

order by advertiser_name, publisher_name, site_name, traffic_week;



