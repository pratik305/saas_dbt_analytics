

with source as (

    select *
    from workspace.default.bronze_subscriptions

),

cleaned as (

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
        ingestion_ts

    from source

    where subscription_id is not null
      and account_id is not null

)

select *
from cleaned