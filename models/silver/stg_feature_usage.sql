{{ config(materialized='view') }}

with source as (

    select *
    from workspace.default.bronze_feature_usage
    where subscription_id is not null

),

aggregated as (

    select
        subscription_id,
        usage_date,
        feature_name,

        sum(usage_count)              as total_usage_count,
        sum(usage_duration_secs)      as total_usage_duration_secs,
        sum(error_count)              as total_error_count,

        max(is_beta_feature)          as is_beta_feature

    from source
    group by
        subscription_id,
        usage_date,
        feature_name

)

select *
from aggregated
