
  
  
  create or replace view `workspace`.`analytics`.`stg_accounts`
  
  as (
    with source as (

    select *
    from workspace.default.bronze_accounts

),

cleaned as (

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
        churn_flag,
        ingestion_ts

    from source

    where account_id is not null

)

select *
from cleaned
  )
