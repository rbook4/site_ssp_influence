WITH ssp AS (
  SELECT
  LOWER(a.online_profile_id) AS online_profile_id,
  product_sku,
  FROM `prj-dfad-31-usrda-p-31.rbook4_cdm.sub_customer_subscriptions_all_curr_vw` AS a
  LEFT JOIN `prj-dfad-31-usrda-p-31.rbook4_cdm.vfc_vehicle_master_vw` AS b
  ON a.vin = b.vin
  WHERE a.subscription_start_date BETWEEN '2024-10-01' AND '2025-03-31'
  AND LOWER(b.vehicle_make) = 'ford'
  AND LOWER(a.product_type_code) = 'paid'
  AND LOWER(a.event_type) != 'b2cauto'
  AND LOWER(a.ssp_country_code) = 'usa'
  GROUP BY ALL
)

SELECT
product_sku,
COUNT(DISTINCT online_profile_id) AS guids
FROM ssp
GROUP BY 1
ORDER BY 2 DESC
