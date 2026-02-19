{{ config(materialized='table') }}

/*
    Grain: one row per account
    Primary gold model for account health dashboards and CSM tooling.
    Source of truth for account-level KPIs.
*/

with base as (

    select * from {{ ref('int_account_360') }}

),

final as (

    select
        -- Identity
        account_id,
        account_name,
        industry,
        country,
        referral_source,
        signup_date,

        -- Current state
        coalesce(current_plan_tier, original_plan_tier)         as plan_tier,
        seats,
        is_trial,
        churn_flag,
        active_subscriptions > 0                                as is_active_customer,

        -- Revenue
        coalesce(current_mrr, 0)                                as current_mrr,
        coalesce(total_arr, 0)                                  as total_arr,
        coalesce(peak_mrr, 0)                                   as peak_mrr,
        ever_upgraded,
        ever_downgraded,

        -- Engagement
        distinct_features_used,
        total_usage_count,
        total_usage_hours,
        active_days,
        top_feature,
        last_usage_date,
        beta_features_used,
        error_rate_pct,

        -- Support
        total_tickets,
        avg_resolution_time_hours,
        avg_satisfaction_score,
        escalation_rate_pct,
        last_ticket_date,

        -- Churn context
        churn_date,
        churn_reason,
        churn_path,
        days_to_churn,
        mrr_at_churn,
        refund_amount_usd,

        -- Health
        health_score,
        case
            when churn_flag = true                          then 'Churned'
            when health_score >= 75                         then 'Healthy'
            when health_score >= 50                         then 'At Risk'
            else                                                'Critical'
        end                                                     as health_status,

        -- Metadata
        current_timestamp()                                     as updated_at

    from base

)

select * from final