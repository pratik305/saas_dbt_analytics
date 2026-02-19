{{ config(materialized='view') }}

/*
    Grain: one row per account
    Aggregates support ticket metrics per account.
 */

with tickets as (

    select
        ticket_id,
        account_id,
        submitted_at,
        closed_at,
        resolution_time_hours,
        priority,
        first_response_time_minutes,
        satisfaction_score,
        escalation_flag

    from {{ ref('stg_support_tickets') }}

),

aggregated as (

    select
        account_id,

        -- Volume
        count(ticket_id)                                        as total_tickets,
        count(case when priority = 'urgent' then 1 end)         as urgent_tickets,
        count(case when priority = 'high' then 1 end)           as high_priority_tickets,
        count(case when escalation_flag = true then 1 end)      as escalated_tickets,

        -- Resolution quality
        round(avg(resolution_time_hours), 2)                    as avg_resolution_time_hours,
        round(avg(first_response_time_minutes), 2)              as avg_first_response_time_minutes,
        round(avg(satisfaction_score), 2)                       as avg_satisfaction_score,
        min(satisfaction_score)                                 as min_satisfaction_score,

        -- Rates
        round(
            count(case when escalation_flag = true then 1 end) * 100.0
            / nullif(count(ticket_id), 0), 2
        )                                                       as escalation_rate_pct,

        -- Recency
        max(submitted_at)                                       as last_ticket_date,
        min(submitted_at)                                       as first_ticket_date

    from tickets
    group by account_id

)

select * from aggregated