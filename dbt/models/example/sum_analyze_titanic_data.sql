
{{ 
    config(
        materialized='table',
        labels = {"team": "analytics"}
    ) 
}}

    select 
        sum(age) as sum_age,
        sum(mean_fare) as sum_mean_fare,
    from {{ ref ('analyze_titanic_data') }}
