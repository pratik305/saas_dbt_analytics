{{ config(materialized='table') }}

/*
    Grain: one row per feature per month
    Tracks feature adoption trends, error rates, and engagement by plan tier.
    Used for product analytics and feature prioritization.
*/

with usage as (

    select
        u.subscription_id,
        u.usage_date,
        u.feature_name,
        u.total_usage_count,
        u.total_usage_duration_secs,
        u.total_error_count,
        u.is_beta_feature

    from {{ ref('stg_feature_usage') }} u

),

subscriptions as (

    select
        subscription_id,
        account_id,
        plan_tier

    from {{ ref('stg_subscriptions') }}

),

-- Join usage to account/plan context
enriched as (

    select
        u.feature_name,
        date_trunc('month', u.usage_date)                       as usage_month,
        s.plan_tier,
        s.account_id,
        u.total_usage_count,
        u.total_usage_duration_secs,
        u.total_error_count,
        u.is_beta_feature

    from usage u
    inner join subscriptions s using (subscription_id)

),

aggregated as (

    select
        feature_name,
        usage_month,
        plan_tier,
        is_beta_feature,

        -- Reach
        count(distinct account_id)                              as active_accounts,

        -- Volume
        sum(total_usage_count)                                  as total_usage_count,
        round(avg(total_usage_count), 2)                        as avg_usage_per_account,

        -- Duration
        round(sum(total_usage_duration_secs) / 3600.0, 2)      as total_usage_hours,
        round(avg(total_usage_duration_secs) / 60.0, 2)        as avg_session_minutes,

        -- Quality
        sum(total_error_count)                                  as total_errors,
        round(
            sum(total_error_count) * 100.0
            / nullif(sum(total_usage_count), 0), 2
        )                                                       as error_rate_pct,

        current_timestamp()                                     as updated_at

    from enriched
    group by
        feature_name,
        usage_month,
        plan_tier,
        is_beta_feature

)

select * from aggregated
order by usage_month desc, active_accounts desc