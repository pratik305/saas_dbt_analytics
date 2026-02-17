
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select signup_date
from `workspace`.`analytics`.`stg_accounts`
where signup_date is null



  
  
      
    ) dbt_internal_test