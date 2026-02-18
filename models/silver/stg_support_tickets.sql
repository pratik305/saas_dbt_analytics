{{ config(materialized='view') }}

with source as (

    select *
    from workspace.default.bronze_support_tickets

),

cleaned as (

    select
        ticket_id,
        account_id,
        submitted_at,
        closed_at,
        resolution_time_hours,
        priority,
        first_response_time_minutes,
        satisfaction_score,
        escalation_flag,
        ingestion_ts

    from source

    where ticket_id is not null
        and account_id is not null

)

select *
from cleaned
