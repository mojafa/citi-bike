{{
    config(
        materialized="table",
        unique_key="ride_id",
        partition_by={
            "field": "started_at",
            "data_type": "timestamp",
            "granularity": "day",
        },
        cluster_by="start_station_name",
    )
}}

with
    citibike_data as (
        select * from {{ source("staging", "rides") }} where ride_id is not null
    )

select
    -- ride info
    {{ dbt_utils.surrogate_key(["ride_id", "started_at"]) }} as r_id,
    cast(ride_id as string) as ride_id,
    cast(rideable_type as string) as rideable_type,
    cast(member_casual as string) as membership_status,

    -- Timestamp
    cast(started_at as timestamp) as started_at,
    cast(ended_at as timestamp) as ended_at,

    -- station info
    cast(start_station_name as string) as start_station_name,
    cast(start_station_id as string) as start_station_id,
    cast(end_station_name as string) as end_station_name,
    cast(end_station_id as string) as end_station_id,

    -- Station info: geo-spatial convert to geospatial in google-studio
    cast(start_lat as numeric) as start_lat,
    cast(start_lng as numeric) as start_lng,
    cast(end_lat as numeric) as end_lat,
    cast(end_lng as numeric) as end_lng

from citibike_data

-- dbt build --select stg_citibike_data.sql --var 'is_test_run: false'
{% if var("is_test_run", default=true) %} limit 100 {% endif %}
