{{ config(materialized='view') }}

/*
    Grain: one row per churn event
    Enriches churn events with account and subscription context (plan at churn, MRR lost).
    Used by: gold_churn_analysis, int_account_360
*/

with churn as (

    select
        churn_event_id,
        account_id,
        churn_date,
        reason_code,
        refund_amount_usd,
        preceding_upgrade_flag,
        preceding_downgrade_flag,
        is_reactivation,
        feedback_text

    from {{ ref('stg_churn_events') }}

),

accounts as (

    select
        account_id,
        account_name,
        industry,
        country,
        plan_tier     as account_plan_tier,
        seats,
        signup_date

    from {{ ref('stg_accounts') }}

),

-- Get the most recent subscription per account to capture MRR lost at churn
latest_subscriptions as (

    select
        account_id,
        plan_tier     as subscription_plan_tier,
        mrr_amount    as mrr_at_churn,
        arr_amount    as arr_at_churn,
        billing_frequency,
        row_number() over (
            partition by account_id
            order by start_date desc
        ) as rn

    from {{ ref('stg_subscriptions') }}

),

enriched as (

    select
        c.churn_event_id,
        c.account_id,
        a.account_name,
        a.industry,
        a.country,
        a.signup_date,

        -- Tenure at churn (days from signup to churn)
        datediff(c.churn_date, a.signup_date) as days_to_churn,

        c.churn_date,
        c.reason_code,
        c.refund_amount_usd,
        c.preceding_upgrade_flag,
        c.preceding_downgrade_flag,
        c.is_reactivation,
        c.feedback_text,

        -- Revenue context
        coalesce(s.subscription_plan_tier, a.account_plan_tier) as plan_tier_at_churn,
        s.mrr_at_churn,
        s.arr_at_churn,
        s.billing_frequency,

        -- Segment churn risk signal
        case
            when c.preceding_downgrade_flag = true then 'downgrade_to_churn'
            when c.preceding_upgrade_flag = true  then 'upgrade_to_churn'
            else 'direct_churn'
        end as churn_path

    from churn c
    left join accounts a using (account_id)
    left join latest_subscriptions s
        on c.account_id = s.account_id and s.rn = 1

)

select * from enriched