{{ config(materialized='view') }}

/*
    Grain: one row per subscription
    Joins accounts with subscriptions to build subscription-level revenue context.
    Used by: gold_mrr_trends, int_account_360
*/

with accounts as (

    select
        account_id,
        account_name,
        industry,
        country,
        signup_date,
        referral_source

    from {{ ref('stg_accounts') }}

),

subscriptions as (

    select
        subscription_id,
        account_id,
        start_date,
        end_date,
        plan_tier,
        seats,
        mrr_amount,
        arr_amount,
        is_trial,
        upgrade_flag,
        downgrade_flag,
        churn_flag,
        billing_frequency,
        auto_renew_flag,

        -- Derived fields
        case
            when end_date is null then true
            else false
        end as is_active,

        datediff(
            coalesce(end_date, current_date()),
            start_date
        ) as subscription_duration_days

    from {{ ref('stg_subscriptions') }}

),

joined as (

    select
        s.subscription_id,
        s.account_id,
        a.account_name,
        a.industry,
        a.country,
        a.signup_date,
        a.referral_source,
        s.start_date,
        s.end_date,
        s.plan_tier,
        s.seats,
        s.mrr_amount,
        s.arr_amount,
        s.is_trial,
        s.upgrade_flag,
        s.downgrade_flag,
        s.churn_flag,
        s.billing_frequency,
        s.auto_renew_flag,
        s.is_active,
        s.subscription_duration_days

    from subscriptions s
    left join accounts a using (account_id)

)

select * from joined