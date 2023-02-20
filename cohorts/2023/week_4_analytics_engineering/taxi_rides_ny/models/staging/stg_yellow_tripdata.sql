{{ config(materialized='view', engine='MergeTree()',
          order_by='pickup_datetime', partition_by='toYYYYMM(pickup_datetime)') }}

with tripdata as 
(
  select *,
    row_number() over(partition by `VendorID`, tpep_pickup_datetime) as rn
  from {{ source('staging', 'yellow_taxi_data') }}
  where `VendorID` is not null 
)
select
   -- identifiers
    -- dbt_utils.surrogate_key doesn't for clickhouse 
    lower(hex(MD5(concat(
      ifNull(toString(vendorid), ''),
      '-',
      ifNull(toString(pickup_datetime), '')
    )))) as tripid,

    toUInt32(`VendorID`) as vendorid,
    toUInt32(`RatecodeID`) as ratecodeid,
    toUInt32(`PULocationID`) as  pickup_locationid,
    toUInt32(`PULocationID`) as dropoff_locationid,
    
    -- timestamps
    tpep_pickup_datetime as pickup_datetime,
    tpep_dropoff_datetime as dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    toInt32(passenger_count) as passenger_count,
    toFloat32(trip_distance) as trip_distance,
    -- yellow cabs are always street-hail
    1 as trip_type,
    
    -- payment info
    toFloat32(fare_amount) as fare_amount,
    toFloat32(extra) as extra,
    toFloat32(mta_tax) as mta_tax,
    toFloat32(tip_amount) as tip_amount,
    toFloat32(tolls_amount) as tolls_amount,
    toFloat32(0.0) as ehail_fee,
    toFloat32(improvement_surcharge) as improvement_surcharge,
    toFloat32(total_amount) as total_amount,
    toInt32(payment_type) as payment_type,
    toString({{ get_payment_type_description('payment_type') }}) as payment_type_description, 
    toFloat32(congestion_surcharge) as congestion_surcharge
from tripdata
where rn = 1

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}

