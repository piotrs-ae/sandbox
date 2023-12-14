WITH total_monthly_nsgmv AS (
    SELECT 
        EXTRACT(MONTH FROM date) AS month
        , sum(gmv_proceeds_usd_50_0_50) AS total_nsgmv
    FROM deep_purple.cds.agg_growth_v2
    WHERE 1=1
    AND user_subscription_package_order = 1
    AND date(date) > current_date() - 180
    GROUP BY 1
)

, cross_device_define AS (
    SELECT 
        blended_id
        , MAX(operating_system_new) OVER (PARTITION BY blended_id ORDER BY session_ts ASC) AS first_system
        , MAX(operating_system_new) OVER (PARTITION BY blended_id ORDER BY session_ts DESC) AS last_system
        , MAX(device_type_new) OVER (PARTITION BY blended_id ORDER BY session_ts ASC) AS first_device_type
        , MAX(device_type_new) OVER (PARTITION BY blended_id ORDER BY session_ts DESC) AS last_device_type
        , MAX(payment_device) OVER (PARTITION BY blended_id ORDER BY session_ts ASC) AS first_payment_device
        , MAX(payment_device) OVER (PARTITION BY blended_id ORDER BY session_ts DESC) AS last_payment_device
    FROM deep_purple.cds.agg_growth_v2
    WHERE 1=1
    AND user_subscription_package_order = 1
)

, filter_for_condition AS (
    SELECT 
    * 
    FROM deep_purple.cds.agg_growth_v2 a 
    WHERE 1=1
    AND date(date) > current_date() - 180
    AND a.blended_id IN (SELECT blended_id FROM cross_device_define WHERE first_system = 'Android' AND last_payment_device IN ('desktop'))
    AND user_subscription_package_order = 1
)

SELECT

EXTRACT(MONTH FROM date) AS month
, sum(gmv_proceeds_usd_50_0_50) AS nsgmv
, sum(gmv_proceeds_usd_50_0_50) / max(tm.total_nsgmv) AS nsgmv_share

FROM filter_for_condition a
LEFT JOIN total_monthly_nsgmv tm 
    ON EXTRACT(MONTH FROM a.date) = tm.month
WHERE 1=1
AND user_subscription_package_order = 1
AND date(date) > current_date() - 180

GROUP BY 1
ORDER BY 1 DESC;
