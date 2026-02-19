{{ config(materialized='table') }}

/*
    Grain: one row per account
    Rule-based customer segmentation for sales, marketing, and CS targeting.
    Segments are derived from health score, MRR, churn risk, and engagement signals.
*/

with base as (

    select * from {{ ref('int_account_360') }}

    where churn_flag = false  -- active accounts only

),

segmented as (

    select
        account_id,
        account_name,
        industry,
        country,
        coalesce(current_plan_tier, original_plan_tier)         as plan_tier,
        coalesce(current_mrr, 0)                                as current_mrr,
        health_score,
        distinct_features_used,
        active_days,
        total_tickets,
        avg_satisfaction_score,
        escalation_rate_pct,
        ever_upgraded,
        ever_downgraded,
        signup_date,
        last_usage_date,

        -- Days since last activity
        datediff(current_date(), last_usage_date)               as days_since_last_activity,

        -- Primary segment
        case
            when health_score >= 80
                and coalesce(current_mrr, 0) >= 1000
                and ever_upgraded = true                        then 'Champion'

            when health_score >= 75
                and coalesce(current_mrr, 0) >= 500             then 'Loyal'

            when health_score >= 60
                and datediff(current_date(), signup_date) <= 90  then 'Promising'

            when health_score >= 50
                and datediff(current_date(), signup_date) <= 30  then 'New Customer'

            when health_score < 50
                and ever_downgraded = true                      then 'At Risk – Downgraded'

            when health_score < 40
                and datediff(current_date(), last_usage_date) > 30
                                                                then 'Hibernating'

            when health_score < 30                              then 'Critical – Likely to Churn'

            else                                                     'Needs Attention'
        end                                                     as customer_segment,

        -- Recommended action
        case
            when health_score >= 80
                and coalesce(current_mrr, 0) >= 1000
                and ever_upgraded = true                        then 'Upsell / Advocacy program'

            when health_score >= 75
                and coalesce(current_mrr, 0) >= 500             then 'Nurture and expand'

            when health_score >= 60
                and datediff(current_date(), signup_date) <= 90  then 'Onboarding check-in'

            when health_score >= 50
                and datediff(current_date(), signup_date) <= 30  then 'Send welcome sequence'

            when health_score < 50
                and ever_downgraded = true                      then 'CSM outreach – retention offer'

            when health_score < 40
                and datediff(current_date(), last_usage_date) > 30
                                                                then 'Re-engagement campaign'

            when health_score < 30                              then 'Urgent CSM intervention'

            else                                                     'Monitor and check in'
        end                                                     as recommended_action,

        current_timestamp()                                     as updated_at

    from base

)

select * from segmented
order by health_score asc  -- surface at-risk accounts first