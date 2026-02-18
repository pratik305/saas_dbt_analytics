{{ config(materialized='view') }}

with source as (

    select *
    from workspace.default.bronze_churn_events

),

cleaned as (

    select
        churn_event_id,
        account_id,
        churn_date,
        reason_code,
        refund_amount_usd,
        preceding_upgrade_flag,
        preceding_downgrade_flag,
        is_reactivation,
        feedback_text,
        ingestion_ts

    from source

    where churn_event_id is not null
        and account_id is not null

)

select *
from cleaned
