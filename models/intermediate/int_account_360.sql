{{ config(materialized='table') }}

/*
    Grain: one row per account
    The central account spine joining all intermediate models.
    Materialized as table because it's an expensive join used by multiple gold models.
    Used by: gold_account_health_summary, gold_customer_segments
*/

with accounts as (

    select
        account_id,
        account_name,
        industry,
        country,
        signup_date,
        referral_source,
        plan_tier,
        seats,
        is_trial,
        churn_flag

    from {{ ref('stg_accounts') }}

),

-- Aggregate subscriptions to account level
subscription_summary as (

    select
        account_id,
        count(subscription_id)                                          as total_subscriptions,
        sum(mrr_amount)                                                 as total_mrr,
        sum(arr_amount)                                                 as total_arr,
        max(mrr_amount)                                                 as peak_mrr,
        count(case when is_active = true then 1 end)                    as active_subscriptions,
        max(case when is_active = true then plan_tier end)              as current_plan_tier,
        max(case when is_active = true then mrr_amount end)             as current_mrr,
        bool_or(upgrade_flag)                                           as ever_upgraded,
        bool_or(downgrade_flag)                                         as ever_downgraded,
        max(start_date)                                                 as latest_subscription_start

    from {{ ref('int_account_subscriptions') }}
    group by account_id

),

-- Latest churn event per account
churn_summary as (

    select
        account_id,
        churn_date,
        reason_code                                                     as churn_reason,
        refund_amount_usd,
        churn_path,
        days_to_churn,
        mrr_at_churn

    from {{ ref('int_account_churn') }}
    qualify row_number() over (partition by account_id order by churn_date desc) = 1

),

support as (

    select * from {{ ref('int_account_support') }}

),

feature_usage as (

    select * from {{ ref('int_account_feature_usage') }}

),

joined as (

    select
        -- Account core
        a.account_id,
        a.account_name,
        a.industry,
        a.country,
        a.signup_date,
        a.referral_source,
        a.plan_tier          as original_plan_tier,
        a.seats,
        a.is_trial,
        a.churn_flag,

        -- Subscription summary
        s.total_subscriptions,
        s.total_mrr,
        s.total_arr,
        s.peak_mrr,
        s.active_subscriptions,
        s.current_plan_tier,
        s.current_mrr,
        s.ever_upgraded,
        s.ever_downgraded,
        s.latest_subscription_start,

        -- Churn context (null if not churned)
        c.churn_date,
        c.churn_reason,
        c.refund_amount_usd,
        c.churn_path,
        c.days_to_churn,
        c.mrr_at_churn,

        -- Support metrics
        coalesce(t.total_tickets, 0)                                    as total_tickets,
        t.avg_resolution_time_hours,
        t.avg_satisfaction_score,
        coalesce(t.escalation_rate_pct, 0)                             as escalation_rate_pct,
        t.last_ticket_date,

        -- Feature usage metrics
        coalesce(u.distinct_features_used, 0)                          as distinct_features_used,
        coalesce(u.total_usage_count, 0)                               as total_usage_count,
        u.total_usage_hours,
        coalesce(u.error_rate_pct, 0)                                  as error_rate_pct,
        coalesce(u.beta_features_used, 0)                              as beta_features_used,
        u.top_feature,
        u.last_usage_date,
        coalesce(u.active_days, 0)                                     as active_days,

        -- Computed health score (0–100)
        -- Higher is healthier. Weighted across usage, support satisfaction, and retention signals.
        round(
            least(100,
                -- Usage engagement (40 pts max)
                least(40, coalesce(u.active_days, 0) * 0.5)
                -- Satisfaction (30 pts max)
                + coalesce(t.avg_satisfaction_score, 3) / 5.0 * 30
                -- No escalations (15 pts)
                + case when coalesce(t.escalation_rate_pct, 0) = 0 then 15 else 0 end
                -- Feature breadth (15 pts max)
                + least(15, coalesce(u.distinct_features_used, 0) * 1.5)
            ), 2
        )                                                               as health_score

    from accounts a
    left join subscription_summary s using (account_id)
    left join churn_summary c using (account_id)
    left join support t using (account_id)
    left join feature_usage u using (account_id)

)

select * from joined