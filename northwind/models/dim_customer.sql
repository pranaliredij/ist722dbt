with stg_customers as (
    -- Pull data from the 'Customers' table in the 'northwind' source
    select * 
    from {{ source('northwind', 'Customers') }}
)

select
    -- Generate a surrogate key using the 'customerid' column
    {{ dbt_utils.generate_surrogate_key(['stg_customers.customerid']) }} as customerkey,
    stg_customers.*
from stg_customers
