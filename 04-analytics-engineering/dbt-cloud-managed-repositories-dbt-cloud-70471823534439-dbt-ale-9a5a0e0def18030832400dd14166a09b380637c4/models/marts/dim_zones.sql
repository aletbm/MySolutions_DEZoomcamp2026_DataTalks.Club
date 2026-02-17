SELECT
    LocationID     AS location_id,
    Borough        AS borough,
    Zone           AS zone,
    service_zone   AS service_zone
FROM {{ source('ny_taxi', 'zones') }}
