{% snapshot snap_subscriptions %}

{{
    config(
        target_schema='snapshots',
        unique_key='subscription_id',
        strategy='check',
        check_cols=[
            'plan_tier',
            'mrr_amount',
            'arr_amount',
            'seats',
            'churn_flag',
            'upgrade_flag',
            'downgrade_flag',
            'auto_renew_flag'
        ]
    )
}}

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

from {{ ref('stg_subscriptions') }}

{% endsnapshot %}