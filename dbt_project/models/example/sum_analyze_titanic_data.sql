
{{ config(materialized='table') }}

    select 
        sum(age) as sum_age,
        sum(mean_fare) as mean_fare,
    from {{ ref ('analyze_titanic_data') }}
