with stg_employees as (
    -- Load all data from the 'Employees' table in the 'northwind' source
    select * 
    from {{ source('northwind', 'Employees') }}
),
stg_supervisors as (
    -- Load all data from the 'Employees' table again for the supervisor information
    select * 
    from {{ source('northwind', 'Employees') }}
)

select
    -- Generate a surrogate key using the employee ID
    {{ dbt_utils.generate_surrogate_key(['e.employeeid']) }} as employeekey,
    e.employeeid,
    
    -- Generate employee names in different formats
    concat(e.lastname, ', ', e.firstname) as employeenamelastfirst,
    concat(e.firstname, ' ', e.lastname) as employeenamefirstlast,
    
    -- Include the employee's title
    e.title as employeetitle,
    
    -- Generate supervisor names in different formats
    concat(s.lastname, ', ', s.firstname) as supervisornamelastfirst,
    concat(s.firstname, ' ', s.lastname) as supervisornamefirstlast

from stg_employees e
-- Join supervisors based on the 'reportsto' field
left join stg_supervisors s 
on e.reportsto = s.employeeid
