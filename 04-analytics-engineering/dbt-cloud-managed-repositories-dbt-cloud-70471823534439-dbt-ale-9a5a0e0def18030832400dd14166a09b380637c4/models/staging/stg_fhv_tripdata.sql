-- models/staging/stg_fhv_tripdata.sql

with source as (

    select *
    from {{ source('ny_taxi', 'fhv_tripdata') }}

),

filtered as (
    select *
    from source
    where dispatching_base_num is not null
),

renamed as (
    select
        dispatching_base_num         as dispatching_base_num,
        pickup_datetime              as pickup_datetime,
        dropOff_datetime             as dropoff_datetime,
        PUlocationID                 as pickup_location_id,
        DOlocationID                 as dropoff_location_id,
        SR_Flag                      as sr_flag,
        Affiliated_base_number       as affiliated_base_number
    from filtered
)

select *
from renamed
