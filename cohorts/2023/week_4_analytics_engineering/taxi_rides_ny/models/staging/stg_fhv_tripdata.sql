{{ config(materialized='view') }}

select
    -- identifiers
    -- dbt_utils.surrogate_key doesn't for clickhouse
    lower(hex(MD5(concat(
      ifNull(toString(vendorid), ''),
      '-',
      ifNull(toString(pickup_datetime), '')
    )))) as tripid,

    dispatching_base_num as vendorid,
    toUInt32(`PULocationID`) as pickup_locationid,
    toUInt32(`PULocationID`) as dropoff_locationid,

    --timestamps
    toDateTime(pickup_datetime) as pickup_datetime,
    toDateTime(dropoff_datetime) as dropoff_datetime,

    --trip info
    `SR_Flag` as sr_flag
from {{ source('staging', 'fhv_taxi_data')}}


-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}