{{ config(materialized='view') }}

SELECT 
    -- identifier
    dispatching_base_num,
    cast(pulocationid as integer) as pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,

    --timestamp
    cast(Pickup_datetime as timestamp) as pickup_datetime,
    cast(DropOff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    sr_flag,
    affiliated_base_number
    
FROM {{ source('staging','fhv_tripdata_2019_bq') }}


-- dbt build --vars '{"is_test_run": "false"}'
{% if var('is_test_run', true) %}
    limit 100
{% endif %} 