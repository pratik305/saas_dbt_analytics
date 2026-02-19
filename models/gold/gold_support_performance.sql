{{ config(materialized='table') }}

/*
    Grain: one row per month per priority level
    SLA and support quality metrics for operational dashboards.
    Used by customer success and support team leadership.
*/

with tickets as (

    select
        ticket_id,
        account_id,
        submitted_at,
        resolution_time_hours,
        priority,
        first_response_time_minutes,
        satisfaction_score,
        escalation_flag,
        date_trunc('month', submitted_at)                       as ticket_month

    from {{ ref('stg_support_tickets') }}

),

final as (

    select
        ticket_month,
        priority,

        -- Volume
        count(ticket_id)                                        as total_tickets,
        count(distinct account_id)                              as accounts_with_tickets,
        count(case when escalation_flag = true then 1 end)      as escalated_tickets,

        -- SLA metrics
        round(avg(resolution_time_hours), 2)                    as avg_resolution_time_hours,
        round(percentile_approx(
            resolution_time_hours, 0.5), 2)                     as median_resolution_time_hours,
        round(percentile_approx(
            resolution_time_hours, 0.95), 2)                    as p95_resolution_time_hours,

        round(avg(first_response_time_minutes), 2)              as avg_first_response_minutes,
        round(percentile_approx(
            first_response_time_minutes, 0.5), 2)               as median_first_response_minutes,

        -- Satisfaction
        round(avg(satisfaction_score), 2)                       as avg_satisfaction_score,
        count(case when satisfaction_score >= 4 then 1 end)     as satisfied_tickets,
        count(case when satisfaction_score <= 2 then 1 end)     as dissatisfied_tickets,

        -- Rates
        round(
            count(case when escalation_flag = true then 1 end) * 100.0
            / nullif(count(ticket_id), 0), 2
        )                                                       as escalation_rate_pct,

        round(
            count(case when satisfaction_score is not null then 1 end) * 100.0
            / nullif(count(ticket_id), 0), 2
        )                                                       as csat_response_rate_pct,

        current_timestamp()                                     as updated_at

    from tickets
    group by ticket_month, priority

)

select * from final
order by ticket_month desc, priority