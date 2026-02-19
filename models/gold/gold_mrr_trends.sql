{{ config(materialized='table') }}

/*
    Grain: one row per month
    Core SaaS revenue metrics: new MRR, churned MRR, net MRR, active accounts.
    Used for executive revenue dashboards and board reporting.
*/

with subscriptions as (

    select
        account_id,
        subscription_id,
        start_date,
        end_date,
        plan_tier,
        mrr_amount,
        arr_amount,
        churn_flag,
        upgrade_flag,
        downgrade_flag,
        is_trial

    from {{ ref('int_account_subscriptions') }}

    where is_trial = false  -- exclude trials from revenue metrics

),

-- Generate a month spine from the earliest start_date to today
months as (

    select
        date_trunc('month', add_months(
            (select min(start_date) from subscriptions),
            pos
        )) as month_start

    from (
        select explode(sequence(
            0,
            cast(floor(months_between(
                current_date(),
                (select min(start_date) from subscriptions)
            )) as int),
            1
        )) as pos
    )

),

-- Classify each subscription per month
monthly_subs as (

    select
        date_trunc('month', s.start_date)                       as month_start,
        s.plan_tier,
        s.mrr_amount,
        'new'                                                   as mrr_type

    from subscriptions s

    union all

    select
        date_trunc('month', s.end_date)                         as month_start,
        s.plan_tier,
        -s.mrr_amount                                           as mrr_amount,
        'churned'                                               as mrr_type

    from subscriptions s
    where s.churn_flag = true and s.end_date is not null

    union all

    select
        date_trunc('month', s.start_date)                       as month_start,
        s.plan_tier,
        s.mrr_amount,
        'expansion'                                             as mrr_type

    from subscriptions s
    where s.upgrade_flag = true

    union all

    select
        date_trunc('month', s.start_date)                       as month_start,
        s.plan_tier,
        -s.mrr_amount                                           as mrr_amount,
        'contraction'                                           as mrr_type

    from subscriptions s
    where s.downgrade_flag = true

),

aggregated as (

    select
        month_start,
        plan_tier,
        mrr_type,
        sum(mrr_amount)                                         as mrr_movement,
        count(*)                                                as subscription_count

    from monthly_subs
    where month_start is not null
    group by month_start, plan_tier, mrr_type

),

final as (

    select
        month_start,
        plan_tier,

        -- MRR by type
        sum(case when mrr_type = 'new'         then mrr_movement else 0 end) as new_mrr,
        sum(case when mrr_type = 'churned'     then mrr_movement else 0 end) as churned_mrr,
        sum(case when mrr_type = 'expansion'   then mrr_movement else 0 end) as expansion_mrr,
        sum(case when mrr_type = 'contraction' then mrr_movement else 0 end) as contraction_mrr,
        sum(mrr_movement)                                                     as net_mrr_change,

        -- Subscription counts
        sum(case when mrr_type = 'new'     then subscription_count else 0 end) as new_subscriptions,
        sum(case when mrr_type = 'churned' then subscription_count else 0 end) as churned_subscriptions,

        current_timestamp()                                                   as updated_at

    from aggregated
    group by month_start, plan_tier

)

select * from final
order by month_start, plan_tier