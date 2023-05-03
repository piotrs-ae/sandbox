
--=========== DIM TABLE ON A ATTRIBUTION UIDS LEVEL==================
--
--Schemas Accessed: CDS, DDS
--Developer: piotr.sieminski
--Goal: To get all actual uids (devices) we are going to use to attribute payments, tutors or users to marketing campaigns
--Output: etl_satge.dim_attribution_uid
--step 1: Get all approvel tutors
--step 2: Get the subscription_tutoring_id_user_normalised field that identifies the subscription tutorings (packages) a user has and all their related payments
--step 3: Calculate the delta GMV for each subscription upgrade by checking how many following payments that upgrades contributes to
--step 4: add new subscribers ltv and lth metrics
--step 5: Get payments that will be attributed to marketing channels: Trials, first package/subscription, upgrades/restarts, topups
--step 6: Union payments with approved tutors and all those other users that haven't paid or are not an approved tutor
--step 7: Get list of devices uid associated with the list of items to attribute

--Step1: Get all approved tutors

CREATE OR REPLACE TEMP TABLE tmp_tutors_approval AS(
    SELECT 
        t.id AS id
        , t.user_id
        , MIN(COALESCE(ttv.first_appr, tfa.first_approval_date)) AS first_approved_ts
        , MIN(reg_completed_ts) AS reg_completed_ts    
    FROM
        DEEP_PURPLE.dds.tutors_tutor t
    LEFT JOIN (
            SELECT 
                ttv.tutor_id
                , MIN(change_date) AS first_appr 
            FROM 
                DEEP_PURPLE.dds.tutorinfo_tutorvisibilitystatelog ttv
            JOIN DEEP_PURPLE.dds.tutors_tutor tt
                ON tt.id = ttv.tutor_id 
            JOIN DEEP_PURPLE.dds.user u
                ON u.id = tt.user_id WHERE u.date_joined >= '2019-02-19 15:24:30.171'
            AND base_data:status::STRING = 'APPROVED' 
            GROUP BY 1
                ) ttv 
        ON ttv.tutor_id = t.id
    LEFT JOIN DEEP_PURPLE.ETL_GLOSSARY.TUTORS_FIRST_APPROVALS tfa
        ON tfa.tutor_id = t.id
    LEFT JOIN (
            SELECT 
                ttv.tutor_id
                , MIN(change_date) AS reg_completed_ts 
            FROM 
                DEEP_PURPLE.dds.tutorinfo_tutorvisibilitystatelog ttv
            WHERE base_data:status::STRING != 'APPLICANT' 
            GROUP BY 1
                ) ttr 
        ON ttr.tutor_id = t.id
    GROUP BY 1, 2
    );

--step 2: Get the subscription_tutoring_id_user_normalised field that identifies 
--the subscription tutorings (packages) a user has and all their related payments

CREATE OR REPLACE TEMP TABLE tmp_subscription_tutoring_id_user_normalised AS(
    SELECT tutoring_id
        , user_id
        , payment_type 
        , business_model_parsed
        , tutoring_first_subscription_package_ts
        , ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY tutoring_first_subscription_package_ts) AS subscription_tutoring_id_user_normalised
    FROM (SELECT tutoring_id
                , user_id
                , payment_type
                , CASE WHEN business_model IN ('SUBS_TUTOR','SUBS_TOPUP') THEN 'SUBS_BM' ELSE NULL END AS business_model_parsed
                , MIN(payment_ts) AS tutoring_first_subscription_package_ts
        FROM DEEP_PURPLE.cds.fact_payment p
        WHERE payment_type = 'PACKAGE' 
                AND business_model IN ('SUBS_TUTOR','SUBS_TOPUP')
        GROUP BY 1, 2 ,3, 4
        )
 );

--Step3: Calculate the delta GMV for each payment by checking how many following payments that upgrades contributes to

CREATE OR REPLACE TEMP TABLE tmp_payments AS (
    SELECT 
        *  
        , md5(concat(p.user_id,p.tutor_id,p.tutoring_id)) AS unic_id 
        , CASE WHEN source IN (
            'SUBS_TUTOR',
            'SUBS_TUTOR_CRON',
            'SUBS_UPGRADE',
            'SUBS_RESTART'
            ) AND payment_type!='TRIAL' 
            THEN ROW_NUMBER() OVER (
                PARTITION BY unic_id
                , CASE 
                    WHEN source IN (
                        'SUBS_TUTOR',
                        'SUBS_TUTOR_CRON',
                        'SUBS_UPGRADE',
                        'SUBS_RESTART'
                        ) AND payment_type!='TRIAL' 
                    THEN tutoring_id 
                    ELSE NULL 
                END ORDER BY payment_ts) 
            ELSE NULL 
            END as tutoring_subscription_payment_order
    FROM DEEP_PURPLE.cds.fact_payment p
);

CREATE OR REPLACE TEMP TABLE tmp_agg_payments AS (
    SELECT 
        p1.id 
        , p1.payment_ts
        , md5(concat(p1.user_id,p1.tutor_id,p1.tutoring_id)) AS unic_id
        , p1.tutoring_payment_order
        , p1.tutoring_subscription_payment_order
        , p1.hours
        , p1.tutoring_id
        , p1.gmv_proceeds_usd
        , MIN(p2.payment_ts) AS valid_payments
    FROM tmp_payments p1
    LEFT JOIN DEEP_PURPLE.cds.fact_payment p2 
        ON md5(concat(p1.user_id,p1.tutor_id,p1.tutoring_id))=md5(concat(p2.user_id,p2.tutor_id,p2.tutoring_id))
        AND p1.payment_ts<=p2.payment_ts  
        AND p2.hours<p1.hours  
        AND p1.tutoring_subscription_payment_order>1 
        AND p2.source IN (
            'SUBS_TUTOR',
            'SUBS_TUTOR_CRON',
            'SUBS_UPGRADE',
            'SUBS_RESTART'
            )
    WHERE p1.source IN (
        'SUBS_TUTOR',
        'SUBS_TUTOR_CRON',
        'SUBS_UPGRADE',
        'SUBS_RESTART'
    )
    GROUP BY 1,2,3,4,5,6,7,8
);

CREATE OR REPLACE TEMP TABLE tmp_payment_min_hours AS (
    SELECT 
    * 
    , MIN(px.hours) OVER (
        PARTITION BY md5(concat(px.user_id,px.tutor_id,px.tutoring_id))
        , CASE 
            WHEN px.tutoring_payment_order=1 THEN TRUE 
            ELSE FALSE 
        END 
        ORDER BY px.payment_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS min_hours
    FROM DEEP_PURPLE.cds.fact_payment px
    WHERE px.source IN (
        'SUBS_TUTOR',
        'SUBS_TUTOR_CRON',
        'SUBS_UPGRADE',
        'SUBS_RESTART'
        )
);


CREATE OR REPLACE TEMP TABLE tmp_delta_gmv AS (
    SELECT 
        m.id
        , SUM(px.gmv_proceeds_usd) as gmv
        , SUM( 
        CASE 
            WHEN  px.hours=px.min_hours AND m.hours=px.hours THEN DIV0(px.gmv_proceeds_usd,px.hours)
            WHEN  px.hours=px.min_hours AND m.hours!=px.hours THEN DIV0(px.gmv_proceeds_usd,m.hours)
            WHEN  px.hours!=px.min_hours AND m.tutoring_subscription_payment_order>1 THEN DIV0(px.gmv_proceeds_usd,m.hours)
            WHEN  px.hours!=px.min_hours  THEN DIV0(px.gmv_proceeds_usd,m.hours)*px.min_hours/px.hours
            ELSE NULL 
        END) AS gmv_per_hour_valid
        , SUM( 
        CASE 
            WHEN  px.hours=px.min_hours AND m.hours=px.hours THEN DIV0(px.gross_margin_usd,px.hours)
            WHEN  px.hours=px.min_hours AND m.hours!=px.hours THEN DIV0(px.gross_margin_usd,m.hours)
            WHEN  px.hours!=px.min_hours AND m.tutoring_subscription_payment_order>1 THEN DIV0(px.gross_margin_usd,m.hours)
            WHEN  px.hours!=px.min_hours  THEN DIV0(px.gross_margin_usd,m.hours)*px.min_hours/px.hours
            ELSE NULL 
        END) AS gm_per_hour_valid
    FROM tmp_agg_payments m
    LEFT JOIN tmp_payment_min_hours px 
        ON m.unic_id=md5(concat(px.user_id,px.tutor_id,px.tutoring_id))  
            AND m.payment_ts<=px.payment_ts  
            AND px.payment_ts<nvl(m.valid_payments,'9999-12-31')  
            AND m.tutoring_payment_order>1     
    GROUP BY m.id
);

--step 4: add new subscribers ltv and lth metrics

CREATE OR REPLACE TEMP TABLE new_subscribers_ltv_and_lth AS (
    SELECT 
    
     p.user_id 
     -- filtering out for the LTV/LTH over X amount of days
    , sum(
            CASE 
                WHEN (
                    DATEDIFF('second', (s.first_subscription_package_ts), (p.payment_ts)) <= 360*24*60*60 
                    AND DATEDIFF('day', (s.first_subscription_package_ts), CURRENT_DATE()) > 360
                    ) THEN gross_margin_usd
                ELSE NULL
            END
    ) AS new_subscribers_ltv_360d
    , sum(
        round(
            CASE 
                WHEN (
                    DATEDIFF('second', (s.first_subscription_package_ts), (p.payment_ts)) <= 360*24*60*60 
                    AND DATEDIFF('day', (s.first_subscription_package_ts), CURRENT_DATE()) > 360
                    ) THEN p.hours
                ELSE NULL
            END
        , 2)
    ) AS new_subscribers_lth_360d
    , sum(
            CASE 
                WHEN (
                    DATEDIFF('second', (s.first_subscription_package_ts), (p.payment_ts)) <= 180*24*60*60
                    AND DATEDIFF('day', (s.first_subscription_package_ts), CURRENT_DATE()) > 180
                    ) THEN gross_margin_usd
                ELSE NULL
            END
    ) AS new_subscribers_ltv_180d
    , sum(
        round(
            CASE 
                WHEN (
                    DATEDIFF('second', (s.first_subscription_package_ts), (p.payment_ts)) <= 180*24*60*60 
                    AND DATEDIFF('day', (s.first_subscription_package_ts), CURRENT_DATE()) > 180
                    ) THEN p.hours
                ELSE NULL
            END
        , 2)
    ) AS new_subscribers_lth_180d
    , sum(
            CASE 
                WHEN (
                    DATEDIFF('second', (s.first_subscription_package_ts), (p.payment_ts)) <= 90*24*60*60 
                    AND DATEDIFF('day', (s.first_subscription_package_ts), CURRENT_DATE()) > 90
                    ) THEN gross_margin_usd
                ELSE NULL
            END
    ) AS new_subscribers_ltv_90d
    , sum(
        round(
            CASE 
                WHEN (
                    DATEDIFF('second', (s.first_subscription_package_ts), (p.payment_ts)) <= 90*24*60*60 
                    AND DATEDIFF('day', (s.first_subscription_package_ts), CURRENT_DATE()) > 90
                    ) THEN p.hours
                ELSE NULL
            END
        , 2)
    ) AS new_subscribers_lth_90d
    , sum(
            CASE 
                WHEN (
                    DATEDIFF('second', (s.first_subscription_package_ts), (p.payment_ts)) <= 30*24*60*60 
                    AND DATEDIFF('day', (s.first_subscription_package_ts), CURRENT_DATE()) > 30
                    ) THEN gross_margin_usd
                ELSE NULL
            END
    ) AS new_subscribers_ltv_30d
    , sum(
        round(
            CASE 
                WHEN (
                    DATEDIFF('second', (s.first_subscription_package_ts), (p.payment_ts)) <= 30*24*60*60 
                    AND DATEDIFF('day', (s.first_subscription_package_ts), CURRENT_DATE()) > 30
                    ) THEN p.hours
                ELSE NULL
            END
        , 2)
    ) AS new_subscribers_lth_30d
    , sum(
            CASE 
                WHEN (
                    DATEDIFF('second', (s.first_subscription_package_ts), (p.payment_ts)) <= 7*24*60*60 
                    AND DATEDIFF('day', (s.first_subscription_package_ts), CURRENT_DATE()) > 7
                    ) THEN gross_margin_usd
                ELSE NULL
            END
    ) AS new_subscribers_ltv_7d
    , sum(
        round(
            CASE 
                WHEN (
                    DATEDIFF('second', (s.first_subscription_package_ts), (p.payment_ts)) <= 7*24*60*60 
                    AND DATEDIFF('day', (s.first_subscription_package_ts), CURRENT_DATE()) > 7
                    ) THEN p.hours
                ELSE NULL
            END
        , 2)
    ) AS new_subscribers_lth_7d
    
    FROM DEEP_PURPLE.CDS.FACT_PAYMENT p
    LEFT JOIN DEEP_PURPLE.CDS.DIM_STUDENT s ON p.student_id = s.id
    WHERE p.business_model IN (
            'SUBS_TUTOR'
            , 'GROUP'
        )
        AND s.first_subscription_package_ts IS NOT NULL
        AND p.is_converted_tutoring
    GROUP BY 1
);

--step 5: Get the accumulated delta GMV and filter the payments that will be attributed to marketing channels

CREATE OR REPLACE TEMP TABLE tmp_fact_payment AS (
    SELECT 
        fp.id
        , fp.user_id
        , payment_ts
        , tutoring_id
        , tutor_id
        , md5(concat(fp.user_id,tutor_id,tutoring_id)) AS unic_id
        , subject
        , source
        , platform
        , payment_type
        , operating_system
        , business_model
        , subscription_frequency
        , student_payment_order
        , tutor_payment_order
        , tutoring_payment_order
        , tutoring_trial_order
        , tutoring_package_order
        , tutoring_subscription_package_order
        , corp_name
        , user_tutoring_order
        , user_trial_order
        , user_package_order
        , user_subscription_trial_order
        , user_marketplace_trial_order
        , user_group_class_trial_order
        , user_subscription_package_order
        , user_marketplace_package_order
        , user_group_class_package_order
        , CASE WHEN business_model = 'GROUP' THEN TRUE ELSE FALSE END AS flag_group_class
        , hours
        , gmv_proceeds_usd
        , gross_margin_usd
        , gross_revenue_usd
        , revenue_projected_usd
        , lt.new_subscribers_ltv_360d
        , lt.new_subscribers_lth_360d
        , lt.new_subscribers_ltv_180d
        , lt.new_subscribers_lth_180d
        , lt.new_subscribers_ltv_90d
        , lt.new_subscribers_lth_90d
        , lt.new_subscribers_ltv_30d
        , lt.new_subscribers_lth_30d
        , lt.new_subscribers_ltv_7d
        , lt.new_subscribers_lth_7d
        , NVL(LAG(
                CASE 
                    WHEN source NOT IN (
                        'SUBS_TUTOR',
                        'SUBS_TUTOR_CRON',
                        'SUBS_UPGRADE',
                        'SUBS_RESTART'
                        ) OR tutoring_payment_order=1 
                        THEN NULL 
                    ELSE hours 
                END
                ) IGNORE NULLS OVER (PARTITION BY unic_id ORDER BY payment_ts),0) as prev_hours
    FROM DEEP_PURPLE.cds.fact_payment fp
    LEFT JOIN new_subscribers_ltv_and_lth lt 
    ON fp.user_id = lt.user_id 
    AND fp.user_subscription_package_order = 1
);

CREATE OR REPLACE TEMP TABLE tmp_subscription_upgrades AS (
    SELECT *
    --Calculate how many upgrades the subscription has
    , COUNT(
            CASE 
                WHEN source IN (
                    'SUBS_UPGRADE',
                    'SUBS_RESTART'
                    ) OR (
                        hours>prev_hours 
                        AND tutoring_payment_order>2
                        ) 
                THEN id 
                ELSE NULL 
            END
            ) OVER (PARTITION BY unic_id ORDER BY payment_ts) AS upgrades_counter
    FROM tmp_fact_payment fp
);

CREATE OR REPLACE TEMP TABLE tmp_upgrade_calculations AS (
    SELECT 
    *
    --Size of the subscription before the upgrade
    , FIRST_VALUE(prev_hours) OVER (PARTITION BY unic_id,upgrades_counter ORDER BY payment_ts) as prev_subs_upgrade_hours
    --Will be used on the last filter only to keep the records when the upgrade happens
    , LAG(upgrades_counter) OVER (PARTITION BY unic_id ORDER BY payment_ts) as prev_upgrades_counter
    --When tutoring had the first marketplace package
    , MIN(
        CASE 
            WHEN tutoring_payment_order>1 
            AND source IN (
                'PREPLY_MARKETPLACE',
                'REC_LESSON',
                'REFILL',
                NULL,
                'PRIVATE'
                ) 
            THEN tutoring_payment_order 
            ELSE NULL 
        END
        ) OVER (PARTITION BY unic_id) AS first_marketplace_package
    --When tutoring had the first group package
    , MIN(
        CASE 
            WHEN tutoring_payment_order>1 
            AND source IN (
                'GROUP',
                'GROUP_V2'
                ) 
            THEN tutoring_payment_order 
            ELSE NULL 
        END
        ) OVER (PARTITION BY unic_id) AS first_group_package
    FROM tmp_subscription_upgrades
);


CREATE OR REPLACE TEMP TABLE tmp_attribution_payments AS (
    SELECT 
        p.id
        , p.user_id
        , p.payment_ts
        , p.tutoring_id
        , p.tutor_id
        , p.unic_id
        , p.subject
        , p.source
        , p.platform
        , p.payment_type
        , p.operating_system
        , p.business_model
        , p.subscription_frequency
        , p.student_payment_order
        , p.tutor_payment_order
        , p.tutoring_payment_order
        , p.tutoring_subscription_package_order
        , p.tutoring_trial_order
        , p.tutoring_package_order
        , p.corp_name
        , p.user_tutoring_order
        , p.user_marketplace_trial_order
        , p.user_subscription_trial_order
        , p.user_marketplace_package_order
        , p.user_subscription_package_order
        , p.user_trial_order
        , p.user_package_order
        , p.flag_group_class
        , p.user_group_class_trial_order
        , p.user_group_class_package_order
        , p.hours
        , p.gmv_proceeds_usd
        , p.gross_margin_usd
        , p.gross_revenue_usd
        , p.revenue_projected_usd
        , p.new_subscribers_ltv_360d
        , p.new_subscribers_lth_360d
        , p.new_subscribers_ltv_180d
        , p.new_subscribers_lth_180d
        , p.new_subscribers_ltv_90d
        , p.new_subscribers_lth_90d
        , p.new_subscribers_ltv_30d
        , p.new_subscribers_lth_30d
        , p.new_subscribers_ltv_7d
        , p.new_subscribers_lth_7d
        , p.first_marketplace_package
        , p.first_group_package
        , p.upgrades_counter
        , p.prev_upgrades_counter
        , f.gmv_per_hour_valid
        , f.gm_per_hour_valid
        , CASE 
            -- GMV of the Subscription topups
            WHEN p.source='SUBS_TOPUP' THEN p.gmv_proceeds_usd
            -- GMV of the Trial
            WHEN p.tutoring_payment_order =1 THEN p.gmv_proceeds_usd
            -- SUM all the GMV subscription upgrades/packages
            WHEN p.source IN (
                'SUBS_TUTOR',
                'SUBS_TUTOR_CRON',
                'SUBS_UPGRADE',
                'SUBS_RESTART'
                ) THEN 
                    CASE 
                        WHEN p.hours-p.prev_subs_upgrade_hours <= 0 THEN 0 
                        ELSE p.hours-p.prev_subs_upgrade_hours 
                    END * f.gmv_per_hour_valid
            ELSE 
            -- SUM all the GMV for group/ marketplace packages
                SUM(p.gmv_proceeds_usd) OVER (
                                            PARTITION BY unic_id,  
                                                CASE 
                                                    WHEN p.source IN (
                                                        'GROUP',
                                                        'GROUP_V2'
                                                        ) THEN 'GROUP' 
                                                    WHEN p.source IN (
                                                        'PREPLY_MARKETPLACE',
                                                        'REC_LESSON',
                                                        'REFILL',
                                                        NULL,
                                                        'PRIVATE'
                                                        ) THEN 'PRPELY_MARKETPLACE' 
                                                    ELSE 'FALSE' 
                                                END 
                                                ORDER BY p.payment_ts ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                                            ) 
        END AS cumulative_gmv_proceeds_usd
        , CASE 
            -- GMV of the Subscription topups
            WHEN p.source='SUBS_TOPUP' THEN p.gross_margin_usd
            -- GMV of the Trial
            WHEN p.tutoring_payment_order =1 THEN p.gross_margin_usd
            -- SUM all the GMV subscription upgrades/packages
            WHEN p.source IN (
                'SUBS_TUTOR',
                'SUBS_TUTOR_CRON',
                'SUBS_UPGRADE',
                'SUBS_RESTART'
                ) THEN 
                    CASE 
                        WHEN p.hours-p.prev_subs_upgrade_hours <= 0 THEN 0 
                        ELSE p.hours-p.prev_subs_upgrade_hours 
                    END * f.gm_per_hour_valid
            ELSE 
            -- SUM all the GMV for group/ marketplace packages
                SUM(p.gross_margin_usd) OVER (
                                            PARTITION BY unic_id,  
                                                CASE 
                                                    WHEN p.source IN (
                                                        'GROUP',
                                                        'GROUP_V2'
                                                        ) THEN 'GROUP' 
                                                    WHEN p.source IN (
                                                        'PREPLY_MARKETPLACE',
                                                        'REC_LESSON',
                                                        'REFILL',
                                                        NULL,
                                                        'PRIVATE'
                                                        ) THEN 'PRPELY_MARKETPLACE' 
                                                    ELSE 'FALSE' 
                                                END 
                                                ORDER BY p.payment_ts ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                                            ) 
        END AS cumulative_gross_margin_usd
    FROM tmp_upgrade_calculations p
    --Aggregate the delta GMV attributed to each package/subscription/upgrade
    LEFT JOIN tmp_delta_gmv f ON f.id=p.id
);

--step 6: Union payments with approved tutors and all those other users that haven't paid or are not an approved tutor
CREATE OR REPLACE TEMP TABLE tmp_attribution_items AS (
    --Add all payments
    SELECT 
    --User dimensions
        user_id
        , u.date_joined AS join_ts
        , 'paying_user' AS user_type
        , NULL AS lr_w_lead
        , NULL AS sign_up_student
    --Blended dimensions
        , payment_ts AS blended_ts
        , md5(concat('paying_user',user_id, payment_ts)) AS blended_id
    --Payment dimensions
        , payment_ts
        , tutoring_id
        , tutor_id
        , subject AS payment_subject
        , source AS payment_source
        , platform as payment_device
        , payment_type
        , operating_system
        , business_model
        , subscription_frequency
        , student_payment_order
        , tutor_payment_order
        , tutoring_payment_order
        , tutoring_subscription_package_order
        , tutoring_trial_order
        , tutoring_package_order
        , corp_name
        , user_tutoring_order
        , user_marketplace_trial_order
        , user_marketplace_package_order
        , user_subscription_trial_order
        , user_subscription_package_order
        , user_group_class_trial_order
        , user_group_class_package_order
        , user_trial_order
        , user_package_order
        , flag_group_class 
    --Financial metrics
        , gmv_proceeds_usd 
        , gross_margin_usd 
        , gross_revenue_usd
        , revenue_projected_usd
        , new_subscribers_ltv_360d
        , new_subscribers_lth_360d
        , new_subscribers_ltv_180d
        , new_subscribers_lth_180d
        , new_subscribers_ltv_90d
        , new_subscribers_lth_90d
        , new_subscribers_ltv_30d
        , new_subscribers_lth_30d
        , new_subscribers_ltv_7d
        , new_subscribers_lth_7d
        , cumulative_gmv_proceeds_usd AS tutoring_gmv_proceeds_usd
        , cumulative_gross_margin_usd AS tutoring_gross_margin_usd
    --Tutor dimensions
        , NULL AS first_approved_ts
        , NULL AS reg_completed_ts
    FROM tmp_attribution_payments
    LEFT JOIN deep_purple.dds.USER u 
    	ON user_id = u.id
    UNION 
    -- Add all tutors approved
    SELECT 
    --User dimensions
        user_id
        , u.date_joined AS join_ts
        , 'approved_tutor' AS user_type
        , NULL AS lr_w_lead
        , NULL AS sign_up_student
    --Blended dimensions
        , reg_completed_ts AS blended_ts
        , md5(concat('approved_tutor',user_id, nvl(nvl(first_approved_ts,reg_completed_ts),'9999-12-31'))) AS blended_id
    --Payment dimensions
        , NULL AS payment_ts
        , NULL AS tutoring_id
        , NULL AS tutor_id
        , NULL AS payment_subject
        , NULL AS payment_source
        , NULL AS payment_device
        , NULL AS payment_type
        , NULL AS operating_system
        , NULL AS business_model
        , NULL AS subscription_frequency
        , NULL AS student_payment_order
        , NULL AS tutor_payment_order
        , NULL AS tutoring_payment_order
        , NULL AS tutoring_subscription_package_order
        , NULL AS tutoring_trial_order
        , NULL AS tutoring_package_order
        , NULL AS corp_name
        , NULL AS user_tutoring_order
        , NULL AS user_marketplace_trial_order
        , NULL AS user_marketplace_package_order
        , NULL AS user_subscription_trial_order
        , NULL AS user_subscription_package_order
        , NULL AS user_group_class_trial_order
        , NULL AS user_group_class_package_order
        , NULL AS user_trial_order
        , NULL AS user_package_order
        , NULL AS flag_group_class
    --Financial metrics
        , NULL AS gmv_proceeds_usd
        , NULL AS gross_margin_usd
        , NULL AS gross_revenue_usd
        , NULL AS revenue_projected_usd
        , NULL AS new_subscribers_ltv_360d
        , NULL AS new_subscribers_lth_360d
        , NULL AS new_subscribers_ltv_180d
        , NULL AS new_subscribers_lth_180d
        , NULL AS new_subscribers_ltv_90d
        , NULL AS new_subscribers_lth_90d
        , NULL AS new_subscribers_ltv_30d
        , NULL AS new_subscribers_lth_30d
        , NULL AS new_subscribers_ltv_7d
        , NULL AS new_subscribers_lth_7d
        , NULL AS tutoring_gmv_proceeds_usd
        , NULL AS tutoring_gross_margin_usd
    --Tutor dimensions
        , first_approved_ts 
        , reg_completed_ts
    FROM tmp_tutors_approval
    LEFT JOIN deep_purple.dds.USER u 
    	ON user_id = u.id
    UNION
    -- Add all those users that have never make a payment and are not approved tutors, but we want to get the attribution information on dim_student or other tables
    SELECT 
    --User dimensions
        u.id AS user_id
        , date_joined as join_ts
        , 'sign_up_user' AS user_type
        , tl.client_id IS NOT NULL AS lr_w_lead
        , CASE WHEN user_id IN (SELECT user_id FROM tmp_tutors_approval GROUP BY 1) THEN FALSE ELSE TRUE END AS sign_up_student
        -- , CASE 
        --     WHEN t.user_id IS NOT NULL THEN FALSE
        --     ELSE TRUE
        -- END AS sign_up_student
    --Blended dimensions
        , date_joined AS blended_ts
        , md5(concat('sign_up_user',u.id, date_joined)) AS blended_id
    --Payment dimensions
        , NULL AS payment_ts
        , NULL AS tutoring_id
        , NULL AS tutor_id
        , NULL AS payment_subject
        , NULL AS payment_source
        , NULL AS payment_device
        , NULL AS payment_type
        , NULL AS operating_system
        , NULL AS business_model
        , NULL AS subscription_frequency
        , NULL AS student_payment_order
        , NULL AS tutor_payment_order
        , NULL AS tutoring_payment_order
        , NULL AS tutoring_subscription_package_order
        , NULL AS tutoring_trial_order
        , NULL AS tutoring_package_order
        , NULL AS corp_name
        , NULL AS user_tutoring_order
        , NULL AS user_marketplace_trial_order
        , NULL AS user_marketplace_package_order
        , NULL AS user_subscription_trial_order
        , NULL AS user_subscription_package_order
        , NULL AS user_group_class_trial_order
        , NULL AS user_group_class_package_order
        , NULL AS user_trial_order
        , NULL AS user_package_order
        , NULL AS flag_group_class
    --Financial metrics
        , NULL AS gmv_proceeds_usd
        , NULL AS gross_margin_usd
        , NULL AS gross_revenue_usd
        , NULL AS revenue_projected_usd
        , NULL AS new_subscribers_ltv_360d
        , NULL AS new_subscribers_lth_360d
        , NULL AS new_subscribers_ltv_180d
        , NULL AS new_subscribers_lth_180d
        , NULL AS new_subscribers_ltv_90d
        , NULL AS new_subscribers_lth_90d
        , NULL AS new_subscribers_ltv_30d
        , NULL AS new_subscribers_lth_30d
        , NULL AS new_subscribers_ltv_7d
        , NULL AS new_subscribers_lth_7d
        , NULL AS tutoring_gmv_proceeds_usd
        , NULL AS tutoring_gross_margin_usd
    --Tutor dimensions
        , NULL AS first_approved_ts
        , NULL AS reg_completed_ts
    FROM DEEP_PURPLE.dds.user u
    LEFT JOIN (
        SELECT user_id
            , id AS client_id
        FROM DEEP_PURPLE.DDS.TUTORS_CLIENT
        GROUP BY 1, 2
    ) tc ON u.id = tc.user_id
    LEFT JOIN (
        SELECT client_id
        FROM DEEP_PURPLE.DDS.TUTORS_LEAD
        GROUP BY 1
    ) tl ON tc.client_id = tl.client_id
    -- LEFT JOIN tmp_tutors_approval t 
    --     ON u.id = t.user_id
);

--step 7: Get list of devices uid associated with the list of items to attribute

-- BEGIN;

-- TRUNCATE TABLE IF EXISTS DEEP_PURPLE.etl_stage.dim_attribution_uid;

CREATE OR REPLACE TABLE public_enemy.data_strategy.ps_dim_attribution_uid AS (

    SELECT 
    --User dimensions
        st.user_id
        , st.join_ts
        , st.user_type
        , st.lr_w_lead
        , st.sign_up_student
    -- Device dimension
        , eu.uid
    --Blended dimensions
        , st.blended_id
        , st.blended_ts
    --Payment dimensions
        , st.payment_ts
        , st.tutoring_id
        , stiun.subscription_tutoring_id_user_normalised
        , st.tutor_id
        , st.payment_subject
        , st.payment_source
        , st.payment_device
        , st.payment_type
        , st.operating_system
        , st.business_model
        , st.subscription_frequency
        , st.student_payment_order
        , st.tutor_payment_order
        , st.tutoring_payment_order
        , st.tutoring_trial_order
        , st.tutoring_package_order
        , st.tutoring_subscription_package_order
        , st.corp_name
        , st.user_tutoring_order
        , st.user_trial_order
        , st.user_package_order
        , st.user_marketplace_trial_order
        , st.user_marketplace_package_order
        , st.user_subscription_trial_order
        , st.user_subscription_package_order
        , st.user_group_class_trial_order
        , st.user_group_class_package_order
        , st.flag_group_class
    --Financial metrics
        , st.gmv_proceeds_usd
        , st.gross_margin_usd
        , st.gross_revenue_usd
        , st.revenue_projected_usd
        , st.new_subscribers_ltv_360d
        , st.new_subscribers_lth_360d
        , st.new_subscribers_ltv_180d
        , st.new_subscribers_lth_180d
        , st.new_subscribers_ltv_90d
        , st.new_subscribers_lth_90d
        , st.new_subscribers_ltv_30d
        , st.new_subscribers_lth_30d
        , st.new_subscribers_ltv_7d
        , st.new_subscribers_lth_7d
        , st.tutoring_gmv_proceeds_usd
        , st.tutoring_gross_margin_usd
    --Tutor dimensions
        , st.first_approved_ts
        , st.reg_completed_ts
    FROM tmp_attribution_items st
    LEFT JOIN tmp_subscription_tutoring_id_user_normalised stiun ON stiun.tutoring_id=st.tutoring_id
                AND stiun.user_id = st.user_id
                AND stiun.payment_type = st.payment_type
                AND stiun.tutoring_first_subscription_package_ts <= st.payment_ts
                AND st.business_model IN ('SUBS_TUTOR','SUBS_TOPUP')
    LEFT JOIN DEEP_PURPLE.cds.dim_exp_uid eu ON eu.user_id = st.user_id
    AND IFF(st.blended_ts = st.join_ts, 
            st.blended_ts >= eu.created_at - INTERVAL '30 DAYS', -- condition for sign up users only
            st.blended_ts >= eu.created_at -- INTERVAL '30 DAYS' -- condition for all other types of users
            ));
-- COMMIT;

--=========== DIM ATTRIBUTION SESSIONS ON A PAYMENT ID LEVEL==================
--
--Schemas Accessed: CDS, ETL_STAGE
--Developer: oriol.batlle
--Goal: To get all actual sessions used during the attribution of payments, tutors or users to marketing campaigns 
--Output: cds.dim_attribution_sessions
--step 1: Join the attribution uids with the sessions during the time window (30 days or 7 days)
--step 2: Create unknown sessions for those attribution items that doesn't have a session
--step 3: Model the different attribution models (first_touch, last_touch, 50_0_50, ...)

--step 1: Join the attribution uids with the sessions during the time window (30 days or 7 days)
CREATE OR REPLACE TEMP TABLE tmp_dim_attribution_with_session AS (
    SELECT
    --User dimensions
        p.join_ts
        , p.user_type
        , p.lr_w_lead
        , p.sign_up_student
        , p.user_id
    --Blended dimensions
        , p.blended_id
        , p.blended_ts AS date
    --Payment dimensions
        , p.payment_ts
        , p.tutoring_id
        , p.subscription_tutoring_id_user_normalised
        , p.tutor_id
        , p.payment_subject
        , p.payment_source
        , p.payment_device
        , p.payment_type
        , p.operating_system AS payment_operating_system
        , p.business_model
        , p.subscription_frequency
        , p.student_payment_order
        , p.tutor_payment_order
        , p.tutoring_payment_order
        , p.tutoring_subscription_package_order
        , p.tutoring_trial_order
        , p.tutoring_package_order
        , p.corp_name
        , p.user_tutoring_order
        , p.user_marketplace_trial_order
        , p.user_marketplace_package_order
        , p.user_subscription_trial_order
        , p.user_subscription_package_order
        , p.user_group_class_package_order
        , p.user_group_class_trial_order
        , p.user_trial_order
        , p.user_package_order
        , p.flag_group_class
    --Financial metrics
        , p.gmv_proceeds_usd
        , p.gross_margin_usd
        , p.gross_revenue_usd
        , p.revenue_projected_usd
        , p.new_subscribers_ltv_360d
        , p.new_subscribers_lth_360d
        , p.new_subscribers_ltv_180d
        , p.new_subscribers_lth_180d
        , p.new_subscribers_ltv_90d
        , p.new_subscribers_lth_90d
        , p.new_subscribers_ltv_30d
        , p.new_subscribers_lth_30d
        , p.new_subscribers_ltv_7d
        , p.new_subscribers_lth_7d
        , p.tutoring_gmv_proceeds_usd
        , p.tutoring_gross_margin_usd
    --Tutor dimensions
        , p.first_approved_ts
        , p.reg_completed_ts
    --Sessions order/number
        , s.start_ts AS session_ts
    --Sessions dimensions
        , s.preply_session
        , s.request_country as country_code
        , CASE 
            WHEN s.site_version = 1 THEN 'desktop' 
            WHEN s.site_version = 2 THEN 'mobile' 
            WHEN s.site_version =3 THEN 'tablet' 
            WHEN s.site_version = 4 THEN 'app' 
        END as device_type
        , s.browser_family
        , s.os_family AS operating_system
        , s.language AS language_version
        , LOWER(s.path) as path
        , s.traffic_group 
        , s.channel_group
        , s.source_medium
        , s.source
        , s.medium
        , s.channel
        , s.utm_source
        , s.utm_medium
        , s.utm_content
        , s.subject
        , s.subject_group
        , s.traffic_type
    -- PPC DIMENSIONS
        , s.account_name
        , s.account_id
        , s.campaign_name
        , s.campaign_id
        , s.ad_group_name
        , s.ad_group_id
        , s.subchannel
        , s.vertical
        , s.target_device
        , s.is_incentivized as camp_is_incentivized
        , s.camp_language
        , s.camp_locale
        , s.target_subject
        , s.camp_info
        , s.channel_maturity
        , s.affiliate_id::INT AS affiliate_id
        , s.kw_segment
        , s.kw_subsegment
        , s.match_type
        , s.competitor
        , s.location_flag
        , s.target_cpa
    --SEO dimensions
        , s.seo_page_type
        , s.city_name
        , s.city_size
        , s.seo_tag_type
        , s.district_name
    FROM public_enemy.data_strategy.ps_dim_attribution_uid p
    INNER JOIN (
        SELECT * 
        FROM DEEP_PURPLE.cds.DIM_SESSION
        WHERE (channel != 'CRM') OR 
                (
                (channel = 'CRM') 
                AND NOT (campaign_name ILIKE ANY (
                                                    '%student-2-opt-in%',
                                                    '%student_2_opt_in%',
                                                    '%inform-message-student%',
                                                    '%inform_message_student%',
                                                    '%student-no-answer%',
                                                    '%student_no_answer%',
                                                    '%student-received-answer%',
                                                    '%student_received_answer%',
                                                    '%reset-password%',
                                                    '%reset_password%',
                                                     '%sys%'
                                                )
                        )
                )
        ) s
                ON s.init_uid = p.uid
                    AND s.start_ts <= p.blended_ts-- Only sessions before conversion
                    AND DATE(s.start_ts) >= (
                                            DATE(p.blended_ts) - CASE 
                                                                    WHEN p.student_payment_order IS NULL 
                                                                        OR p.user_marketplace_trial_order=1 
                                                                        OR p.user_subscription_trial_order=1 
                                                                        OR p.user_group_class_trial_order=1 
                                                                        OR p.user_marketplace_package_order=1 
                                                                        OR p.user_subscription_package_order=1 
                                                                        OR p.user_group_class_package_order=1 
                                                                        THEN 30 
                                                                        ELSE 7 
                                                                  END
                                                )
);

-- Step 2: Create unknown sessions for those attribution items that doesn't have a session
CREATE OR REPLACE TEMP TABLE tmp_dim_attribution_without_session AS (
    SELECT
    --User dimensions
        au.join_ts
        , au.user_type
        , au.lr_w_lead
        , au.sign_up_student
        , au.user_id
    --Blended dimensions
        , au.blended_id
        , au.blended_ts AS date
    --Payment dimensions
        , au.payment_ts
        , au.tutoring_id
        , au.subscription_tutoring_id_user_normalised
        , au.tutor_id
        , au.payment_subject
        , au.payment_source
        , au.payment_device
        , au.payment_type
        , au.operating_system AS payment_operating_system
        , au.business_model
        , au.subscription_frequency
        , au.student_payment_order
        , au.tutor_payment_order
        , au.tutoring_payment_order
        , au.tutoring_subscription_package_order
        , au.tutoring_trial_order
        , au.tutoring_package_order
        , au.corp_name
        , au.user_tutoring_order
        , au.user_marketplace_trial_order
        , au.user_marketplace_package_order
        , au.user_subscription_trial_order
        , au.user_subscription_package_order
        , au.user_group_class_package_order
        , au.user_group_class_trial_order
        , au.user_trial_order
        , au.user_package_order
        , au.flag_group_class
    --Financial metrics
        , au.gmv_proceeds_usd
        , au.gross_margin_usd
        , au.gross_revenue_usd
        , au.revenue_projected_usd
        , au.new_subscribers_ltv_360d
        , au.new_subscribers_lth_360d
        , au.new_subscribers_ltv_180d
        , au.new_subscribers_lth_180d
        , au.new_subscribers_ltv_90d
        , au.new_subscribers_lth_90d
        , au.new_subscribers_ltv_30d
        , au.new_subscribers_lth_30d
        , au.new_subscribers_ltv_7d
        , au.new_subscribers_lth_7d
        , au.tutoring_gmv_proceeds_usd
        , au.tutoring_gross_margin_usd
    --Tutor dimensions
        , au.first_approved_ts
        , au.reg_completed_ts
    --Sessions order/number
        , NULL AS session_ts
    --Sessions dimensions
        , 'unknown' as preply_session
        , NULL as country_code
        , NULL as device_type
        , NULL AS browser_family
        , NULL AS operating_system
        , NULL AS language_version
        , NULL as path
        , 'unknown' AS traffic_group 
        , 'unknown' AS channel_group
        , 'unknown' AS source_medium
        , 'unknown' AS source
        , 'unknown' AS medium
        , 'unknown' AS channel
        , NULL AS utm_source
        , NULL AS utm_medium
        , NULL AS utm_content
        , NULL AS subject
        , NULL AS subject_group
        , NULL AS traffic_type
    -- PPC DIMENSIONS
        , NULL AS account_name
        , NULL AS account_id
        , NULL AS campaign_name
        , NULL AS campaign_id
        , NULL AS ad_group_name
        , NULL AS ad_group_id
        , NULL AS subchannel
        , NULL AS vertical
        , NULL AS target_device
        , NULL AS camp_is_incentivized
        , NULL AS camp_language
        , NULL AS camp_locale
        , NULL AS target_subject
        , NULL AS camp_info
        , NULL AS channel_maturity
        , NULL AS affiliate_id
        , NULL AS kw_segment
        , NULL AS kw_subsegment
        , NULL AS match_type
        , NULL AS competitor
        , NULL AS location_flag
        , NULL AS target_cpa
    --SEO dimensions
        , NULL AS seo_page_type
        , NULL AS city_name
        , NULL AS city_size
        , NULL AS seo_tag_type
        , NULL AS district_name
    FROM public_enemy.data_strategy.ps_dim_attribution_uid au
    LEFT JOIN tmp_dim_attribution_with_session s ON au.blended_id=s.blended_id
    WHERE s.blended_id IS NULL
);

-- BEGIN;

-- TRUNCATE TABLE DEEP_PURPLE.etl_stage.dim_attribution_sessions;

CREATE OR REPLACE TABLE public_enemy.data_strategy.ps_dim_attribution_sessions AS (
    SELECT    
    --User dimensions
        s.join_ts
        , s.user_type
        , s.lr_w_lead
        , s.sign_up_student
        , s.user_id
    --Blended dimensions
        , s.blended_id
        , s.date
    --Payment dimensions
        , s.payment_ts
        , s.tutoring_id
        , s.subscription_tutoring_id_user_normalised
        , s.tutor_id
        , s.payment_subject
        , s.payment_source
        , s.payment_device
        , s.payment_type
        , s.operating_system AS payment_operating_system
        , s.business_model
        , s.subscription_frequency
        , s.student_payment_order
        , s.tutor_payment_order
        , s.tutoring_payment_order
        , s.tutoring_trial_order
        , s.tutoring_package_order
        , s.tutoring_subscription_package_order
        , s.corp_name
        , s.user_tutoring_order
        , s.user_trial_order
        , s.user_package_order
        , s.user_marketplace_trial_order
        , s.user_marketplace_package_order
        , s.user_subscription_trial_order
        , s.user_subscription_package_order
        , s.user_group_class_package_order
        , s.user_group_class_trial_order
        , s.flag_group_class
    --Financial metrics
        , s.gmv_proceeds_usd
        , s.gross_margin_usd
        , s.gross_revenue_usd
        , s.revenue_projected_usd
        , s.new_subscribers_ltv_360d
        , s.new_subscribers_lth_360d
        , s.new_subscribers_ltv_180d
        , s.new_subscribers_lth_180d
        , s.new_subscribers_ltv_90d
        , s.new_subscribers_lth_90d
        , s.new_subscribers_ltv_30d
        , s.new_subscribers_lth_30d
        , s.new_subscribers_ltv_7d
        , s.new_subscribers_lth_7d
        , s.tutoring_gmv_proceeds_usd
        , s.tutoring_gross_margin_usd
    --Tutor dimensions
        , s.first_approved_ts
        , s.reg_completed_ts
        , aats.affiliate_id::VARCHAR(10)  AS tut_affiliate_id
    --Sessions order/number
        , s.session_ts
        , ROW_NUMBER() OVER (PARTITION BY s.blended_id ORDER BY s.session_ts ASC) AS asc_session_order
        , ROW_NUMBER() OVER (PARTITION BY s.blended_id ORDER BY s.session_ts DESC) AS desc_session_order
        , COUNT(*) OVER (PARTITION BY s.blended_id) as num_session
    -- logic for remove direct and bad CRM touchpoints (the coalesce is used to retrieve the dimension of the cms even when this dimension is null)
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.source_medium, 'null_source_medium') END as sm_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.channel, 'null_channel') END as c_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.traffic_group, 'null_traffic_group') END as tg_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.channel_group, 'null_channel_group') END as channel_group_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.subchannel, 'null_subchannel') END as sc_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.campaign_name, 'null_campaign_name') END as campaign_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.ad_group_name,'null_ad_group_name') END AS ad_group_name_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.account_name,'null_account_name') END AS account_name_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.vertical,'null_vertical') END AS vertical_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.target_device,'null_target_device') END AS target_device_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.camp_is_incentivized,'null_camp_is_incentivized') END AS camp_is_incentivized_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.camp_language,'null_camp_language') END AS camp_language_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.camp_locale,'null_camp_locale') END AS camp_locale_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.target_subject,'null_target_subject') END AS target_subject_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.camp_info,'null_camp_info') END AS camp_info_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.affiliate_id, 000) END AS affiliate_id_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.kw_segment,'null_kw_segment') END AS kw_segment_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.kw_subsegment,'null_kw_subsegment') END AS kw_subsegment_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.match_type,'null_match_type') END AS match_type_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.competitor,'null_competitor') END AS competitor_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.country_code,'null_country_code') END AS country_code_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(dc.focus_countries,'null_focus_countries') END AS focus_countries_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(dc.region,'null_country_region') END AS country_region_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(dc.preply_region,'null_preply_region') END AS preply_region_dir_null   
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.device_type,'null_device_type') END AS device_type_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.browser_family,'null_browser_family') END AS browser_family_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.operating_system,'null_operating_system') END AS operating_system_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.language_version,'null_language_version') END AS language_version_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.path,'null_path') END AS path_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.subject, 'null_subject') END AS subject_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.subject_group,'null_subject_group') END AS subject_group_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.traffic_type,'null_traffic_type') END AS traffic_type_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.seo_page_type,'null_seo_page_type') END AS seo_page_type_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.city_name,'null_city_name') END AS city_name_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.city_size,'null_city_size') END AS city_size_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.seo_tag_type,'null_seo_tag_type') END AS seo_tag_type_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.district_name,'null_district_name') END AS district_name_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.preply_session,'null_preply_session') END AS preply_session_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.account_id,'null_account_id') END AS account_id_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.campaign_id,'null_campaign_id') END AS campaign_id_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE COALESCE(s.ad_group_id,'null_preply_ad_group_id') END AS ad_group_id_dir_null
        , CASE WHEN s.source_medium IN ('(direct) / (none)','system / email') THEN NULL ELSE s.session_ts END AS session_ts_dir_null
    --Sessions dimensions
        , s.preply_session
        , CASE
            WHEN preply_session_dir_null IS NOT NULL THEN preply_session_dir_null
            WHEN LAG(preply_session_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.preply_session,'null_preply_session')
            ELSE LAG(preply_session_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as preply_session_new
        , s.country_code
        , CASE
            WHEN country_code_dir_null IS NOT NULL THEN country_code_dir_null
            WHEN LAG(country_code_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.country_code,'null_country_code')
            ELSE LAG(country_code_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as country_code_new
        , dc.focus_countries
        , CASE
            WHEN focus_countries_dir_null IS NOT NULL THEN focus_countries_dir_null
            WHEN LAG(focus_countries_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(dc.focus_countries,'null_focus_countries')
            ELSE LAG(focus_countries_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as focus_countries_new
        , dc.region as country_region
        , CASE
            WHEN country_region_dir_null IS NOT NULL THEN country_region_dir_null
            WHEN LAG(country_region_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(dc.region,'null_country_region')
            ELSE LAG(country_region_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as country_region_new
        , dc.preply_region
        , CASE
            WHEN preply_region_dir_null IS NOT NULL THEN preply_region_dir_null
            WHEN LAG(preply_region_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(dc.preply_region,'null_preply_region')
            ELSE LAG(preply_region_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as preply_region_new
        , s.device_type
        , CASE
            WHEN device_type_dir_null IS NOT NULL THEN device_type_dir_null
            WHEN LAG(device_type_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.device_type,'null_device_type')
            ELSE LAG(device_type_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as device_type_new
        , s.browser_family
        , CASE
            WHEN browser_family_dir_null IS NOT NULL THEN browser_family_dir_null
            WHEN LAG(browser_family_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.browser_family,'null_browser_family')
            ELSE LAG(browser_family_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as browser_family_new
        , s.operating_system
        , CASE
            WHEN operating_system_dir_null IS NOT NULL THEN operating_system_dir_null
            WHEN LAG(operating_system_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.operating_system,'null_operating_system')
            ELSE LAG(operating_system_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as operating_system_new
        , s.language_version
        , CASE
            WHEN language_version_dir_null IS NOT NULL THEN language_version_dir_null
            WHEN LAG(language_version_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.language_version,'null_language_version')
            ELSE LAG(language_version_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as language_version_new
        , s.PATH
        , CASE
            WHEN path_dir_null IS NOT NULL THEN path_dir_null
            WHEN LAG(path_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.path,'null_path')
            ELSE LAG(path_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as path_new
        , s.traffic_group 
        , CASE
            WHEN tg_dir_null IS NOT NULL THEN tg_dir_null
            WHEN LAG(tg_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.traffic_group, 'null_traffic_group')
            ELSE LAG(tg_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as traffic_group_new
        , s.channel_group
        , CASE
            WHEN channel_group_dir_null IS NOT NULL THEN channel_group_dir_null
            WHEN LAG(channel_group_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.channel_group, 'null_channel_group')
            ELSE LAG(channel_group_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as channel_group_new
        , s.source_medium
        , CASE
            WHEN sm_dir_null IS NOT NULL THEN sm_dir_null
            WHEN LAG(sm_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.source_medium, 'null_source_medium')
            ELSE LAG(sm_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as source_medium_new
        , s.source
        , s.medium
        , s.channel
        , CASE
            WHEN c_dir_null IS NOT NULL THEN c_dir_null
            WHEN LAG(c_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.channel, 'null_channel')
            ELSE LAG(c_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as channel_new
        , s.utm_source
        , s.utm_medium
        , s.utm_content
        , s.subject
        , CASE
            WHEN subject_dir_null IS NOT NULL THEN subject_dir_null
            WHEN LAG(subject_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.subject, 'null_subject')
            ELSE LAG(subject_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as subject_new
        , s.subject_group
        , CASE
            WHEN subject_group_dir_null IS NOT NULL THEN subject_group_dir_null
            WHEN LAG(subject_group_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.subject_group,'null_subject_group')
            ELSE LAG(subject_group_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as subject_group_new
        , s.traffic_type
        , CASE
            WHEN traffic_type_dir_null IS NOT NULL THEN traffic_type_dir_null
            WHEN LAG(traffic_type_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.traffic_type,'null_traffic_type')
            ELSE LAG(traffic_type_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as traffic_type_new
    -- PPC DIMENSIONS
        , s.account_name
        , CASE
            WHEN account_name_dir_null IS NOT NULL THEN account_name_dir_null
            WHEN LAG(account_name_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.account_name,'null_account_name')
            ELSE LAG(account_name_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as account_name_new
        , s.account_id
        , CASE
            WHEN account_id_dir_null IS NOT NULL THEN account_id_dir_null
            WHEN LAG(account_id_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.account_id,'null_account_id')
            ELSE LAG(account_id_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END AS account_id_new
        , s.campaign_name
        , CASE
            WHEN campaign_dir_null IS NOT NULL THEN campaign_dir_null
            WHEN LAG(campaign_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.campaign_name, 'null_campaign_name')
            ELSE LAG(campaign_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as campaign_name_new
        , s.campaign_id
        , CASE
            WHEN campaign_id_dir_null IS NOT NULL THEN campaign_id_dir_null
            WHEN LAG(campaign_id_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.campaign_id,'null_campaign_id')
            ELSE LAG(campaign_id_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END AS campaign_id_new
        , s.ad_group_name
        , CASE
            WHEN ad_group_name_dir_null IS NOT NULL THEN ad_group_name_dir_null
            WHEN LAG(ad_group_name_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.ad_group_name,'null_ad_group_name')
            ELSE LAG(ad_group_name_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as ad_group_name_new
        , s.ad_group_id
        , CASE
            WHEN ad_group_id_dir_null IS NOT NULL THEN ad_group_id_dir_null
            WHEN LAG(ad_group_id_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.ad_group_id,'null_ad_group_id')
            ELSE LAG(ad_group_id_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END AS ad_group_id_new
        , s.subchannel
        , CASE
            WHEN sc_dir_null IS NOT NULL THEN sc_dir_null
            WHEN LAG(sc_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.subchannel, 'null_subchannel')
            ELSE LAG(sc_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as subchannel_new
        , s.vertical
        , CASE
            WHEN vertical_dir_null IS NOT NULL THEN vertical_dir_null
            WHEN LAG(vertical_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.vertical,'null_vertical')
            ELSE LAG(vertical_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as vertical_new
        , s.target_device
        , CASE
            WHEN target_device_dir_null IS NOT NULL THEN target_device_dir_null
            WHEN LAG(target_device_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.target_device,'null_target_device')
            ELSE LAG(target_device_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as target_device_new
        , s.camp_is_incentivized
        , CASE
            WHEN camp_is_incentivized_dir_null IS NOT NULL THEN camp_is_incentivized_dir_null
            WHEN LAG(camp_is_incentivized_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.camp_is_incentivized,'null_camp_is_incentivized')
            ELSE LAG(camp_is_incentivized_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as camp_is_incentivized_new
        , s.camp_language
        , CASE
            WHEN camp_language_dir_null IS NOT NULL THEN camp_language_dir_null
            WHEN LAG(camp_language_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.camp_language,'null_camp_language')
            ELSE LAG(camp_language_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as camp_language_new
        , s.camp_locale
        , CASE
            WHEN camp_locale_dir_null IS NOT NULL THEN camp_locale_dir_null
            WHEN LAG(camp_locale_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.camp_locale,'null_camp_locale')
            ELSE LAG(camp_locale_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as camp_locale_new
        , s.target_subject
        , CASE
            WHEN target_subject_dir_null IS NOT NULL THEN target_subject_dir_null
            WHEN LAG(target_subject_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.target_subject,'null_target_subject')
            ELSE LAG(target_subject_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as target_subject_new
        , s.camp_info
        , CASE
            WHEN camp_info_dir_null IS NOT NULL THEN camp_info_dir_null
            WHEN LAG(camp_info_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.camp_info,'null_camp_info')
            ELSE LAG(camp_info_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as camp_info_new
        , s.channel_maturity
        , s.affiliate_id
        , CASE
            WHEN affiliate_id_dir_null IS NOT NULL THEN affiliate_id_dir_null
            WHEN LAG(affiliate_id_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.affiliate_id, 000)
            ELSE LAG(affiliate_id_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as affiliate_id_new
        , s.kw_segment
        , CASE
            WHEN kw_segment_dir_null IS NOT NULL THEN kw_segment_dir_null
            WHEN LAG(kw_segment_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.kw_segment,'null_kw_segment')
            ELSE LAG(kw_segment_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as kw_segment_new
        , s.kw_subsegment
        , CASE
            WHEN kw_subsegment_dir_null IS NOT NULL THEN kw_subsegment_dir_null
            WHEN LAG(kw_subsegment_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.kw_subsegment,'null_kw_subsegment')
            ELSE LAG(kw_subsegment_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as kw_subsegment_new
        , s.match_type
        , CASE
            WHEN match_type_dir_null IS NOT NULL THEN match_type_dir_null
            WHEN LAG(match_type_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.match_type,'null_match_type')
            ELSE LAG(match_type_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as match_type_new
        , s.competitor
        , CASE
            WHEN competitor_dir_null IS NOT NULL THEN competitor_dir_null
            WHEN LAG(competitor_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.competitor,'null_competitor')
            ELSE LAG(competitor_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as competitor_new
        , s.location_flag
        , s.target_cpa
    --SEO dimensions
        , s.seo_page_type
        , CASE
            WHEN seo_page_type_dir_null IS NOT NULL THEN seo_page_type_dir_null
            WHEN LAG(seo_page_type_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.seo_page_type,'null_seo_page_type')
            ELSE LAG(seo_page_type_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as seo_page_type_new
        , s.city_name
        , CASE
            WHEN city_name_dir_null IS NOT NULL THEN city_name_dir_null
            WHEN LAG(city_name_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.city_name,'null_city_name')
            ELSE LAG(city_name_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as city_name_new
        , s.city_size
        , CASE
            WHEN city_size_dir_null IS NOT NULL THEN city_size_dir_null
            WHEN LAG(city_size_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.city_size,'null_city_size')
            ELSE LAG(city_size_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as city_size_new
        , s.seo_tag_type
        , CASE
            WHEN seo_tag_type_dir_null IS NOT NULL THEN seo_tag_type_dir_null
            WHEN LAG(seo_tag_type_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.seo_tag_type,'null_seo_tag_type')
            ELSE LAG(seo_tag_type_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as seo_tag_type_new
        , s.district_name
        , CASE
            WHEN district_name_dir_null IS NOT NULL THEN district_name_dir_null
            WHEN LAG(district_name_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN COALESCE(s.district_name,'null_district_name')
            ELSE LAG(district_name_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as district_name_new
        , CASE
            WHEN session_ts_dir_null IS NOT NULL THEN session_ts_dir_null
            WHEN LAG(session_ts_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts) IS NULL THEN s.session_ts
            ELSE LAG(session_ts_dir_null) IGNORE NULLS OVER (PARTITION BY s.blended_id ORDER BY s.session_ts)
        END as session_ts_new
    --ATTRIBUTION MODEL1: 100/0/0 (FIRST TOUCH)
        , 1 AS first_touch_weight_model1
        , 0 AS last_touch_weight_model1
        , 1-first_touch_weight_model1-last_touch_weight_model1 AS middle_touch_weight_model1
        , CASE 
            WHEN num_session=1 THEN 1
            WHEN num_session=2 
                AND first_touch_weight_model1+last_touch_weight_model1<1 
                AND asc_session_order=1 
                THEN first_touch_weight_model1/(first_touch_weight_model1+last_touch_weight_model1)
            WHEN num_session=2 
                AND first_touch_weight_model1+last_touch_weight_model1<1 
                AND desc_session_order=1 
                THEN last_touch_weight_model1/(first_touch_weight_model1+last_touch_weight_model1)
            WHEN asc_session_order=1 
                AND num_session>1 
                THEN first_touch_weight_model1
            WHEN asc_session_order>1 
                AND desc_session_order>1 
                THEN DIV0(middle_touch_weight_model1,num_session-2)
            WHEN desc_session_order=1 THEN last_touch_weight_model1
        END AS attribution_first_touch_weight
    --ATTRIBUTION MODEL2: 0/0/100 (LAST TOUCH)
        , 0 AS first_touch_weight_model2
        , 1 AS last_touch_weight_model2
        , 1-first_touch_weight_model2-last_touch_weight_model2 AS middle_touch_weight_model2
        , CASE 
            WHEN num_session=1 THEN 1
            WHEN num_session=2 
                AND first_touch_weight_model2+last_touch_weight_model2<1 
                AND asc_session_order=1 
                THEN first_touch_weight_model2/(first_touch_weight_model2+last_touch_weight_model2)
            WHEN num_session=2 
                AND first_touch_weight_model2+last_touch_weight_model2<1 
                AND desc_session_order=1 
                THEN last_touch_weight_model2/(first_touch_weight_model2+last_touch_weight_model2)
            WHEN asc_session_order=1 
                AND num_session>1 
                THEN first_touch_weight_model2
            WHEN asc_session_order>1 
                AND desc_session_order>1 
                THEN DIV0(middle_touch_weight_model2,num_session-2)
            WHEN desc_session_order=1 THEN last_touch_weight_model2
        END AS attribution_last_touch_weight
    --ATTRIBUTION MODEL3: 100/0/0 (LINEAR)
        , NULL AS first_touch_weight_model3
        , NULL AS last_touch_weight_model3
        , NULL AS middle_touch_weight_model3
        , 1/num_session AS attribution_linear_weight
    --ATTRIBUTION MODEL4: 80/20/0 
        , 0.8 AS first_touch_weight_model4
        , 0.0 AS last_touch_weight_model4
        , 1-first_touch_weight_model4-last_touch_weight_model4 AS middle_touch_weight_model4
        , CASE 
            WHEN num_session=1 THEN 1
            WHEN num_session=2 
                AND first_touch_weight_model4+last_touch_weight_model4<1 
                AND asc_session_order=1 
                THEN first_touch_weight_model4/(first_touch_weight_model4+last_touch_weight_model4)
            WHEN num_session=2 
                AND first_touch_weight_model4+last_touch_weight_model4<1 
                AND desc_session_order=1 
                THEN last_touch_weight_model4/(first_touch_weight_model4+last_touch_weight_model4)
            WHEN asc_session_order=1 
                AND num_session>1 
                THEN first_touch_weight_model4
            WHEN asc_session_order>1 
                AND desc_session_order>1 
                THEN DIV0(middle_touch_weight_model4,num_session-2)
            WHEN desc_session_order=1 THEN last_touch_weight_model4
        END AS attribution_80_20_0_weight
    --ATTRIBUTION MODEL5: 40/20/40
        , 0.4 AS first_touch_weight_model5
        , 0.4 AS last_touch_weight_model5
        , 1-first_touch_weight_model5-last_touch_weight_model5 AS middle_touch_weight_model5
        , CASE 
            WHEN num_session=1 THEN 1
            WHEN num_session=2 
                AND first_touch_weight_model5+last_touch_weight_model5<1 
                AND asc_session_order=1 
                THEN first_touch_weight_model5/(first_touch_weight_model5+last_touch_weight_model5)
            WHEN num_session=2 
                AND first_touch_weight_model5+last_touch_weight_model5<1 
                AND desc_session_order=1 
                THEN last_touch_weight_model5/(first_touch_weight_model5+last_touch_weight_model5)
            WHEN asc_session_order=1 
                AND num_session>1 
                THEN first_touch_weight_model5
            WHEN asc_session_order>1 
                AND desc_session_order>1 
                THEN DIV0(middle_touch_weight_model5,num_session-2)
            WHEN desc_session_order=1 THEN last_touch_weight_model5
        END AS attribution_40_20_40_weight
    --ATTRIBUTION MODEL6: 50/0/50
        , 0.5 AS first_touch_weight_model6
        , 0.5 AS last_touch_weight_model6
        , 1-first_touch_weight_model6-last_touch_weight_model6 AS middle_touch_weight_model6
        , CASE 
            WHEN num_session=1 THEN 1
            WHEN num_session=2 
                AND first_touch_weight_model6+last_touch_weight_model6<1 
                AND asc_session_order=1 
                THEN first_touch_weight_model6/(first_touch_weight_model6+last_touch_weight_model6)
            WHEN num_session=2 
                AND first_touch_weight_model6+last_touch_weight_model6<1 
                AND desc_session_order=1 
                THEN last_touch_weight_model6/(first_touch_weight_model6+last_touch_weight_model6)
            WHEN asc_session_order=1 
                AND num_session>1 
                THEN first_touch_weight_model6
            WHEN asc_session_order>1 
                AND desc_session_order>1 
                THEN DIV0(middle_touch_weight_model6,num_session-2)
            WHEN desc_session_order=1 THEN last_touch_weight_model6
        END AS attribution_50_0_50_weight
    FROM (
        SELECT *
        FROM tmp_dim_attribution_with_session
        UNION
        SELECT *
        FROM tmp_dim_attribution_without_session
        ) s
    LEFT JOIN DEEP_PURPLE.etl_glossary.dim_country dc ON s.country_code = dc.country_code
    LEFT JOIN DEEP_PURPLE.cds.dim_tutor dt ON dt.user_id=s.user_id
    LEFT JOIN DEEP_PURPLE.cds.agg_affiliates_tutor_spend aats on dt.id = aats.tutor_id
    );

-- COMMIT;

--=========== AGG TABLE ON A ATTRIBUTION + MARKETING DATA LEVEL (RAW)==================
--
--Schemas Accessed: CDS, ETL_STAGE
--Developer: oriol.batlle
--Goal: To get all information needed to analyze the performance of our marketing campaigns by using different attribution models
--Output: cds.agg_growth_v2
--step 1: Union both tables, attribution sessions and marketing campaigns data (spend, visits, ...)
    
-- BEGIN;

-- TRUNCATE TABLE DEEP_PURPLE.cds.agg_growth_v2;

CREATE OR REPLACE TABLE public_enemy.data_strategy.ps_agg_growth_v2_device_analysis AS (
SELECT
    a.date
--User dimensions
    , a.join_ts
    , a.user_type
    , a.lr_w_lead
    , a.user_id
--Blended dimensions
    , a.blended_id
--Payment dimensions
    , a.payment_ts
    , a.tutoring_id
    , a.subscription_tutoring_id_user_normalised
    , a.tutor_id
    , a.payment_subject
    , a.payment_source
    , a.payment_device
    , a.payment_type
    , a.operating_system AS payment_operating_system
    , a.business_model
    , a.subscription_frequency
    , a.student_payment_order
    , a.tutor_payment_order
    , a.tutoring_payment_order
    , a.tutoring_trial_order
    , a.tutoring_package_order
    , a.tutoring_subscription_package_order
    , a.corp_name
    , a.user_tutoring_order
    , a.user_trial_order
    , a.user_package_order
    , a.user_marketplace_trial_order
    , a.user_marketplace_package_order
    , a.user_subscription_trial_order
    , a.user_subscription_package_order
    , a.user_group_class_package_order
    , a.user_group_class_trial_order
    , a.flag_group_class
--Sessions dimesnions
    , a.session_ts
    , a.asc_session_order
    , a.desc_session_order
    , a.num_session
--Tutor dimensions
    , a.first_approved_ts
    , a.reg_completed_ts
    , a.tut_affiliate_id
--General dimensions
    , a.country_code
    , a.country_code_new
    , a.focus_countries
    , a.focus_countries_new
    , a.country_region
    , a.country_region_new
    , a.preply_region
    , a.preply_region_new
    , a.device_type
    , a.device_type_new
    , a.browser_family
    , a.browser_family_new
    , a.operating_system
    , a.operating_system_new
    , a.language_version
    , a.language_version_new
    , a.path
    , a.path_new
    , a.traffic_group
    , a.traffic_group_new
    , a.channel_group
    , a.channel_group_new
    , a.source_medium
    , a.source_medium_new
    , a.source
    , a.medium
    , a.channel
    , a.channel_new
    , a.utm_source
    , a.utm_medium
    , a.utm_content
    , a.subject
    , a.subject_new
    , a.subject_group
    , a.subject_group_new
    , a.traffic_type
    , a.traffic_type_new
-- PPC DIMENSIONS
    , a.account_name
    , a.account_name_new
    , a.account_id
    , a.account_id_new
    , a.campaign_name
    , a.campaign_name_new
    , a.campaign_id
    , a.campaign_id_new
    , a.ad_group_name
    , a.ad_group_name_new
    , a.ad_group_id
    , a.ad_group_id_new
    , a.subchannel
    , a.subchannel_new
    , a.vertical
    , a.vertical_new
    , a.target_device
    , a.target_device_new
    , a.camp_is_incentivized
    , a.camp_is_incentivized_new
    , a.camp_language
    , a.camp_language_new
    , a.camp_locale
    , a.camp_locale_new
    , a.target_subject
    , a.target_subject_new
    , a.camp_info
    , a.camp_info_new
    , a.channel_maturity
    , a.affiliate_id
    , a.affiliate_id_new
    , a.kw_segment
    , a.kw_segment_new
    , a.kw_subsegment
    , a.kw_subsegment_new
    , a.match_type
    , a.match_type_new
    , a.competitor
    , a.competitor_new
    , a.location_flag
    , a.target_cpa
--SEO dimensions
    , a.seo_page_type
    , a.seo_page_type_new
    , a.city_name
    , a.city_name_new
    , a.city_size
    , a.city_size_new
    , a.seo_tag_type
    , a.seo_tag_type_new
    , a.district_name
    , a.district_name_new
--Financial metrics
    , a.gmv_proceeds_usd
    , a.gross_margin_usd
    , a.new_subscribers_ltv_360d
    , a.new_subscribers_lth_360d
    , a.new_subscribers_ltv_180d
    , a.new_subscribers_lth_180d
    , a.new_subscribers_ltv_90d
    , a.new_subscribers_lth_90d
    , a.new_subscribers_ltv_30d
    , a.new_subscribers_lth_30d
    , a.new_subscribers_ltv_7d
    , a.new_subscribers_lth_7d
    , a.tutoring_gmv_proceeds_usd
    , a.tutoring_gross_margin_usd
--ATTRIBUTION MODEL1: 100/0/0 (FIRST TOUCH)
    , a.attribution_first_touch_weight
    , a.attribution_last_touch_weight
    , a.attribution_linear_weight
    , a.attribution_80_20_0_weight
    , a.attribution_40_20_40_weight
    , a.attribution_50_0_50_weight
-- acquisition metrics
    , NULL AS potential_impressions_volume
    , NULL as impressions
    , NULL as clicks
    , NULL as sessions
    , NULL as visitors
    , NULL as new_visitors
    , NULL as app_installs
    , NULL AS app_installs_new_npc_store_alike
    , NULL as dim_ses_new_visitors
    , NULL AS dim_ses_new_visitors_multiple_views_with_search
    , NULL as new_searchers
    , NULL AS searches
    , NULL AS spend
    , 'dim_attribution_sessions' AS table_name
FROM public_enemy.data_strategy.ps_dim_attribution_sessions a
UNION
SELECT
    m.date
--User dimensions
    , NULL AS join_ts
    , NULL AS user_type
    , NULL AS lr_w_lead
    , NULL AS user_id
--Blended dimensions
    , NULL AS blended_id
--Payment dimensions
    , NULL AS payment_ts
    , NULL AS tutoring_id
    , NULL AS subscription_tutoring_id_user_normalised
    , NULL AS tutor_id
    , NULL AS payment_subject
    , NULL AS payment_source
    , NULL AS payment_device
    , NULL AS payment_type
    , NULL AS payment_operating_system
    , NULL AS business_model
    , NULL AS subscription_frequency
    , NULL AS student_payment_order
    , NULL AS tutor_payment_order
    , NULL AS tutoring_payment_order
    , NULL AS tutoring_trial_order
    , NULL AS tutoring_package_order
    , NULL AS tutoring_subscription_package_order
    , NULL AS corp_name
    , NULL AS user_tutoring_order
    , NULL AS user_trial_order
    , NULL AS user_package_order
    , NULL AS user_marketplace_trial_order
    , NULL AS user_marketplace_package_order
    , NULL AS user_subscription_trial_order
    , NULL AS user_subscription_package_order
    , NULL AS user_group_class_package_order
    , NULL AS user_group_class_trial_order
    , m.flag_group_class
--Sessions dimesnions
    , NULL AS session_ts
    , NULL AS asc_session_order
    , NULL AS desc_session_order
    , NULL AS num_session
--Tutor dimensions
    , NULL AS first_approved_ts
    , NULL AS reg_completed_ts
    , m.tut_affiliate_id
--General dimensions
    , m.country_code
    , COALESCE(m.country_code,'null_country_code') as country_code_new
    , m.focus_countries
    , COALESCE(m.focus_countries,'null_focus_countries') as focus_countries_new 
    , m.country_region
    , COALESCE(m.country_region,'null_country_region') as country_region_new 
    , m.preply_region
    , COALESCE(m.preply_region,'null_preply_region') as preply_region_new
    , m.device_type
    , COALESCE(m.device_type,'null_device_type') as device_type_new 
    , m.browser_family
    , COALESCE(m.browser_family,'null_browser_family') as browser_family_new 
    , m.operating_system
    , COALESCE(m.operating_system,'null_operating_system') as operating_system_new 
    , m.language_version
    , COALESCE(m.language_version,'null_language_version') as language_version_new 
    , m.path
    , COALESCE(m.path,'null_path') AS path_new 
    , m.traffic_group
    , COALESCE(m.traffic_group,'null_traffic_group') AS traffic_group_new
    , m.channel_group
    , COALESCE(m.channel_group,'null_channel_group') AS channel_group_new
    , m.source_medium
    , COALESCE(m.source_medium,'null_source_medium') AS source_medium_new
    , m.source
    , m.medium
    , m.channel
    , COALESCE(m.channel,'null_channel') AS channel_new
    , m.utm_source
    , m.utm_medium
    , m.utm_content
    , m.subject
    , COALESCE(m.subject,'null_subject') as subject_new
    , m.subject_group
    , COALESCE(m.subject_group,'null_subject_group') as subject_group_new 
    , m.traffic_type
    , COALESCE(m.traffic_type,'null_traffic_type') as traffic_type_new 
-- PPC DIMENSIONS
    , m.account_name
    , COALESCE(m.account_name,'null_account_name') AS account_name_new
    , m.account_id 
    , COALESCE(m.account_id,'null_account_id') as account_id_new
    , m.campaign_name
    , COALESCE(m.campaign_name,'null_campaign_name') AS campaign_name_new
    , m.campaign_id
    , COALESCE(m.campaign_id,'null_campaign_id') AS campaign_id_new 
    , m.ad_group_name
    , COALESCE(m.ad_group_name,'null_ad_group_name') AS ad_group_name_new
    , m.ad_group_id
    , COALESCE(m.ad_group_id,'null_ad_group_id') AS ad_group_id_new
    , m.subchannel
    , COALESCE(m.subchannel,'null_subchannel') AS subchannel_new
    , m.vertical
    , COALESCE(m.vertical,'null_vertical') AS vertical_new
    , m.target_device
    , COALESCE(m.target_device,'null_target_device') AS target_device_new 
    , m.camp_is_incentivized
    , COALESCE(m.camp_is_incentivized,'null_camp_is_incentivized') AS camp_is_incentivized_new 
    , m.camp_language
    , COALESCE(m.camp_language,'null_camp_language') AS camp_language_new
    , m.camp_locale
    , COALESCE(m.camp_locale,'null_camp_locale') AS camp_locale_new 
    , m.target_subject
    , COALESCE(m.target_subject,'null_target_subject') AS target_subject_new
    , m.camp_info
    , COALESCE(m.camp_info,'null_camp_info') AS camp_info_new
    , m.channel_maturity
    , m.affiliate_id
    , COALESCE(m.affiliate_id,000) AS affiliate_id_new
    , m.kw_segment
    , COALESCE(m.kw_segment,'null_kw_segment') AS kw_segment_new 
    , m.kw_subsegment
    , COALESCE(m.kw_subsegment,'null_kw_subsegment') AS kw_subsegment_new 
    , m.match_type
    , COALESCE(m.match_type,'null_match_type') AS match_type_new 
    , m.competitor
    , COALESCE(m.competitor,'null_competitor') AS competitor_new 
    , m.location_flag
    , m.target_cpa
--SEO dimensions
    , m.seo_page_type
    , COALESCE(m.seo_page_type,'null_seo_page_type') as seo_page_type
    , m.city_name
    , COALESCE(m.city_name,'null_city_name') as city_name_new 
    , m.city_size
    , COALESCE(m.city_size,'null_city_size') as city_size_new 
    , m.seo_tag_type
    , COALESCE(m.seo_tag_type,'null_seo_tag_type') as seo_tag_type_new 
    , m.district_name 
    , COALESCE(m.district_name,'null_district_name') as district_name_new
--Financial metrics
    , NULL AS gmv_proceeds_usd
    , NULL AS gross_margin_usd
    , NULL AS new_subscribers_ltv_360d
    , NULL AS new_subscribers_lth_360d
    , NULL AS new_subscribers_ltv_180d
    , NULL AS new_subscribers_lth_180d
    , NULL AS new_subscribers_ltv_90d
    , NULL AS new_subscribers_lth_90d
    , NULL AS new_subscribers_ltv_30d
    , NULL AS new_subscribers_lth_30d
    , NULL AS new_subscribers_ltv_7d
    , NULL AS new_subscribers_lth_7d
    , NULL AS tutoring_gmv_proceeds_usd
    , NULL AS tutoring_gross_margin_usd
--ATTRIBUTION MODEL
    , NULL AS attribution_first_touch_weight
    , NULL AS attribution_last_touch_weight
    , NULL AS attribution_linear_weight
    , NULL AS attribution_80_20_0_weight
    , NULL AS attribution_40_20_40_weight
    , NULL AS attribution_50_0_50_weight
-- acquisition metrics
    , m.potential_impressions_volume
    , m.impressions
    , m.clicks
    , m.sessions
    , m.visitors
    , m.new_visitors
    , m.app_installs
    , m.app_installs_new_npc_store_alike
    , m.dim_ses_new_visitors
    , m.dim_ses_new_visitors_multiple_views_with_search
    , m.new_searchers
    , m.searches
    , m.spend
    , 'dim_marketing_campaigns' AS table_name
FROM DEEP_PURPLE.cds.dim_marketing_campaigns m
);
