WITH adobe AS(
  SELECT
  DATE(df0rgf_date_time) AS visit_date,
  LOWER(df0rgf_post_evar46) AS guid,
  CONCAT(df0rgf_post_visid_low, df0rgf_post_visid_high,df0rgf_visit_start_time_gmt,df0rgf_visit_num) AS visit_id,
  MIN(DATE(df0rgf_date_time)) OVER(PARTITION BY LOWER(df0rgf_post_evar46)) AS min_visit_date,
  MAX(DATE(df0rgf_date_time)) OVER(PARTITION BY LOWER(df0rgf_post_evar46)) AS max_visit_date


  FROM `prj-dfad-31-usrda-p-31.rbook4_adobe_clickstream_na1.gdf0rgf_adobe_clickstr_na_t123all_raw_vw`
  -- Date
    WHERE DATE(df0rgf_date_time) BETWEEN '2024-09-01' AND '2025-03-31'

  -- Include Owner Site
  AND df0rgf_post_evar15 = 'ford.com/owners'

  -- Exclude Webview Hits
  AND LOWER(df0rgf_page_url) NOT LIKE '%webview=true%'

  -- cServices Click
  AND (CASE
  WHEN df0rgf_post_prop5 LIKE '%owner:tiles:connected services:manage%' THEN LOWER(df0rgf_post_evar46)
  WHEN df0rgf_post_prop5 LIKE '&referral:fv:profile:subscriptions%' THEN LOWER(df0rgf_post_evar46)
  WHEN df0rgf_post_prop5 LIKE '%home:owning a ford:view connected services%' THEN LOWER(df0rgf_post_evar46)
  WHEN df0rgf_post_prop5 = 'secondary nav:selection:my vehicle:connected services' THEN LOWER(df0rgf_post_evar46)
  WHEN df0rgf_post_prop5 = 'owner:quick links:connected services' THEN LOWER(df0rgf_post_evar46)
  END) IS NOT NULL

  -- Adobe General Filters
  AND df0rgf_exclude_hit = '0'
  AND df0rgf_hit_source NOT IN ('5','7','8','9')

  -- Segment : Exclude Internal GUIDs
  AND IFNULL(df0rgf_post_evar46,'') NOT IN ('89D5CDD8-98AC-4669-8950-37BF95BDB92E','44d639fb-cc5f-4bb3-ab88-b49f1296bc3a','4a4363b4-3fe4-43aa-80e9-45550dbbbbd6','75BCBE0E-B730-446B-AAB6-B98FB44D8D63','75cd773a-0108-3489-9e65-4eeba163fd24','2337FA35-C143-4C6F-9359-820A5560F65D','50584076-5dfd-42ef-9a14-76ce49d3f624','1f764525-509d-4be4-9212-79de71887935','6fb6c7ed-f477-4a57-9e60-8762caab8817','760a28b4-ac77-4691-af91-5ce812bebfda','8120412f-1570-4a98-a1b1-c993f733758c','85cf90ac-a474-4d8a-b7f0-8ac0a87dae99','955d3822-39a6-4cd7-9e9b-bc6641160497','b22d7b3b-dec6-42b5-893b-a6ebe05cd3c2','c755f5eb-1fbf-45c3-b25d-dc9f972ff5ac','d051b5b7-04c3-42d5-8b30-dba7b7dfea45','f9207ee1-fd2a-4295-8d8b-b9ad3890f238','fb9246f1-5bab-4c3f-91b6-2503d357e101','fbbfea62-1a5f-4930-a7d7-6ee49a50455e','382523a4-6c1b-4878-aefd-c81d9d64333b','85cf90ac-a474-4d8a-b7f0-8ac0a87dae99','0273c4c7-e7ff-4f40-b6e2-8e3a5bc952c0','bed0d317-8776-4025-b89d-74bc858b0970','85cf90ac-a474-4d8a-b7f0-8ac0a87dae99','d051b5b7-04c3-42d5-8b30-dba7b7dfea45','d051b5b7-04c3-42d5-8b30-dba7b7dfea45','d051b5b7-04c3-42d5-8b30-dba7b7dfea45','85cf90ac-a474-4d8a-b7f0-8ac0a87dae99','8a98cd45-cfe9-4d16-abac-bef0d9609553','d051b5b7-04c3-42d5-8b30-dba7b7dfea45','0373039b-8a80-4ad3-b009-0d7083f169b0','5296857f-e22d-4926-951b-6adb3346c801','0373039b-8a80-4ad3-b009-0d7083f169b0','d051b5b7-04c3-42d5-8b30-dba7b7dfea45','8120412f-1570-4a98-a1b1-c993f733758c','382523a4-6c1b-4878-aefd-c81d9d64333b','382523a4-6c1b-4878-aefd-c81d9d64333b','b0ee0e91-2a71-42bb-aadc-2ba9e1540d07','9a65be9e-c639-4597-9513-52570d93d070','8120412f-1570-4a98-a1b1-c993f733758c','382523a4-6c1b-4878-aefd-c81d9d64333b','85cf90ac-a474-4d8a-b7f0-8ac0a87dae99','d051b5b7-04c3-42d5-8b30-dba7b7dfea45','85cf90ac-a474-4d8a-b7f0-8ac0a87dae99','1c55d018-d801-4bad-9efc-9267e0269a26','3a181281-fa43-4a80-af49-a48560b10dcc','8c7cd196-f340-46a8-a75f-d214903bec96','a47aeb38-2e22-4fe5-96db-64336ed5baca','a93142ac-b80d-414b-8f12-152b46393231','adf35622-eaa2-40e3-9e32-6dc65febf9c9','d65537a8-1f27-48fd-ace6-9b53444e5c7e','e406e4f7-8841-4002-9171-935a293c7d8e','578bdc1c-8ae8-4f68-bd1d-6562e4a21bd9','af4e61b9-1067-4b62-87ef-64324e9861bb','f90ca1b4-e774-486c-973f-3522b99822f3','3c521c72-464d-47e0-b05a-d306034644ee','90e2dabc-dd47-4dd1-8e94-6a46641d66f4','a52c15bb-a276-498e-ae0f-a9b7c3aa1a1e','b0f0fbef-20a2-45e9-9559-ecaabe257bce','c63e3919-65ac-450d-a364-0dffc0c8b634','9057c266-060b-4dd8-b5a2-becb8fb0df1a','898d5b28-7cef-411c-a159-a3d449ae4a41','15953583-ee01-4d03-a7f1-2e927ec62834','557f3a60-2d0f-4291-82c2-2470af6635c1','d58f72f5-d105-4606-afbe-402ba5ce4f83','e9ef154e-616f-4e90-9a37-4270513f19c6','6ff8183b-6b51-47fb-a514-ad8e7902e2eb','136a6226-5005-a671-0e4b-aa690e4baa69','2b9834d7-02cb-4b20-bd88-7947b6e87b85','4e31c9f2-c2d4-4c60-a5a6-624cdba7785a','f34e146e-3dde-4b8b-b148-a26efe007447','e76d2723-2ce7-43bc-8692-9e776909b45f','c03925cd-d6f5-4bd3-99b5-5b833ae620bb','211a6554-3601-40c1-a411-8e3d4e78b263','83c74208-58d5-4852-a322-66e9f1da49c7','557da819-b5b8-4c3a-897a-50b415e419ee')

  -- Segment: Master Bot Filter
  AND (
          CASE 
              WHEN 
                  df0rgf_post_evar47 = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, LIKE Gecko) Chrome/44.0.2403.157 Safari/537.36'
                  OR df0rgf_post_evar47 = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534+ (KHTML, LIKE Gecko) BingPreview/1.0b'
                  OR df0rgf_post_evar47 = 'Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)'
                  OR df0rgf_post_evar47 = 'Mozilla/5.0 (iPhone; CPU iPhone OS 7_1 LIKE Mac OS X) AppleWebKit/537.51.2 (KHTML, LIKE Gecko) Version/7.0 Mobile/11D167 Safari/9537.53KTXN'
                  OR df0rgf_post_evar47 = 'Mozilla/5.0 (X11; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0'
                  OR df0rgf_post_evar47 LIKE 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, LIKE Gecko) Chrome/49.0.2623.87 Safari/53%'
                  OR df0rgf_post_evar47 LIKE 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, LIKE Gecko) Chrome/55.0.2883.95 Safari/53%'
                  OR df0rgf_post_evar47 = 'Mozilla/5.0 (X11; Linux i686; rv:24.0) Gecko/20100101 Firefox/24.0 DejaClick/2.5.0.7'
                  OR df0rgf_post_evar47 LIKE 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, LIKE Gecko) Chrome/97.0.4692.71 Safari/53%'
                  OR df0rgf_post_evar47 = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, LIKE Gecko) Chrome/99.0.4844.83'
                  OR df0rgf_domain = 'zenlayer.com'
                  OR df0rgf_domain = 'thousandeyes.com'
                  OR df0rgf_domain = 'ipxon.net'
                  OR df0rgf_domain = 'comcAStbusiness.net'
                  OR df0rgf_domain = 'keynote.com'
                  OR df0rgf_domain = 'ford.com'
                  OR df0rgf_domain = 'teamdetroit.com'
                  OR df0rgf_domain = 'perficientgdc.com.cn'
                  OR df0rgf_domain = 'mdp.com'
                  OR df0rgf_visit_referrer = 'http://sparkflow-a.akamaihd.net/spk/1320/28608-1493323333/m5-0.html'
                  OR df0rgf_first_hit_page_url = 'https://jwyatt+demo%40thousandeyes.com:Demohacked22!!@www.ford.com/'
                  OR df0rgf_ref_domain = 'celtra.com'
                  OR df0rgf_va_closer_detail = 'go.dugouthub.com'
                  OR (
                      df0rgf_va_closer_id = '7'
                      AND (df0rgf_visit_start_pagename LIKE '%fv:si:vls:vehicle details%' OR df0rgf_visit_start_pagename LIKE '%fv:si:vls:results%')
                      AND df0rgf_visit_num = '1'
                      AND DATE(df0rgf_date_time) >= '2025-01-21'
                  ) 
              THEN 'Bot'
              ELSE 'NonBot' 
          END
          = 'NonBot'
      )
),

ssp AS (
  SELECT
  LOWER(a.online_profile_id) AS online_profile_id,
  FROM `prj-dfad-31-usrda-p-31.rbook4_cdm.sub_customer_subscriptions_all_curr_vw` AS a
  LEFT JOIN `prj-dfad-31-usrda-p-31.rbook4_cdm.vfc_vehicle_master_vw` AS b
  ON a.vin = b.vin
  WHERE a.subscription_start_date BETWEEN '2024-10-01' AND '2025-03-31'
  AND LOWER(b.vehicle_make) = 'ford'
  AND LOWER(a.product_type_code) = 'paid'
  AND LOWER(a.event_type) != 'b2cauto'
  AND LOWER(a.ssp_country_code) = 'usa'
)

SELECT
-- GUIDs
COUNT(DISTINCT a.guid) AS total_guids_in_adobe,
COUNT(DISTINCT CASE WHEN b.online_profile_id IS NOT NULL THEN a.guid END) AS matched_guids,
COUNT(DISTINCT CASE WHEN b.online_profile_id IS NOT NULL THEN a.guid END) / COUNT(DISTINCT a.guid) AS guids_pct_matched,

-- Visits
COUNT(DISTINCT a.visit_id) AS total_visits_in_adobe,
COUNT(DISTINCT CASE WHEN b.online_profile_id IS NOT NULL THEN a.visit_id END) AS matched_total_visits,
COUNT(DISTINCT CASE WHEN b.online_profile_id IS NOT NULL THEN a.visit_id END) / COUNT(DISTINCT a.visit_id) AS visits_pct_matched

FROM adobe AS a
LEFT JOIN ssp AS b
ON a.guid = b.online_profile_id
GROUP BY ALL
