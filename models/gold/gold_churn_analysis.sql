{{ config(materialized='table') }}

/*
    Grain: one row per churn event
    Used for churn root cause analysis, cohort analysis, and revenue leakage reporting.
*/

with churn as (

    select * from {{ ref('int_account_churn') }}

),

final as (

    select
        -- Event identity
        churn_event_id,
        account_id,
        account_name,

        -- Segmentation
        industry,
        country,
        plan_tier_at_churn,
        billing_frequency,

        -- Timeline
        signup_date,
        churn_date,
        days_to_churn,
        date_trunc('month', churn_date)                         as churn_month,

        -- Tenure bucket
        case
            when days_to_churn < 30     then '< 1 month'
            when days_to_churn < 90     then '1–3 months'
            when days_to_churn < 180    then '3–6 months'
            when days_to_churn < 365    then '6–12 months'
            else                             '12+ months'
        end                                                     as tenure_bucket,

        -- Reason
        reason_code                                             as churn_reason,
        churn_path,
        preceding_upgrade_flag,
        preceding_downgrade_flag,
        is_reactivation,
        feedback_text,

        -- Revenue impact
        coalesce(mrr_at_churn, 0)                               as mrr_lost,
        coalesce(arr_at_churn, 0)                               as arr_lost,
        coalesce(refund_amount_usd, 0)                          as refund_amount_usd,

        -- Metadata
        current_timestamp()                                     as updated_at

    from churn

)

select * from final