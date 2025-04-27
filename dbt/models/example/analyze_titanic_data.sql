
{{ 
    config(
        materialized='table',
        labels = {"team": "analytics"}
    ) 

}}

    select 
        `Age`,
        AVG(`Fare`) as mean_fare,
    from {{ source ('dbt_project', 'titanic_data') }}
    where `Age` is not null
    group by `Age`
    order by `Age`
