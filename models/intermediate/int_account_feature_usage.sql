{{ config(materialized='view') }}

/*
    Grain: one row per account
    Aggregates feature usage across all subscriptions per account.
    Used by: gold_feature_adoption, int_account_360
*/

with usage as (

    select
        subscription_id,
        usage_date,
        feature_name,
        total_usage_count,
        total_usage_duration_secs,
        total_error_count,
        is_beta_feature

    from {{ ref('stg_feature_usage') }}

),

subscriptions as (

    select
        subscription_id,
        account_id

    from {{ ref('stg_subscriptions') }}

),

-- Join usage to accounts via subscriptions
usage_with_account as (

    select
        s.account_id,
        u.subscription_id,
        u.usage_date,
        u.feature_name,
        u.total_usage_count,
        u.total_usage_duration_secs,
        u.total_error_count,
        u.is_beta_feature

    from usage u
    inner join subscriptions s using (subscription_id)

),

aggregated as (

    select
        account_id,

        -- Overall usage
        count(distinct feature_name)                            as distinct_features_used,
        sum(total_usage_count)                                  as total_usage_count,
        round(sum(total_usage_duration_secs) / 3600.0, 2)      as total_usage_hours,
        sum(total_error_count)                                  as total_error_count,
        round(
            sum(total_error_count) * 100.0
            / nullif(sum(total_usage_count), 0), 2
        )                                                       as error_rate_pct,

        -- Beta adoption
        count(distinct case when is_beta_feature = true
            then feature_name end)                              as beta_features_used,

        -- Most used feature
        max_by(feature_name, total_usage_count)                 as top_feature,

        -- Recency
        max(usage_date)                                         as last_usage_date,
        min(usage_date)                                         as first_usage_date,
        count(distinct usage_date)                              as active_days

    from usage_with_account
    group by account_id

)

select * from aggregated