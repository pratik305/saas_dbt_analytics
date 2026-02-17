
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select seats
from `workspace`.`analytics`.`stg_accounts`
where seats is null



  
  
      
    ) dbt_internal_test