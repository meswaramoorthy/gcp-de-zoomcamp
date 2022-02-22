{{ config(materialized='view') }}

select * from {{ source('staging', 'green_tripdata_partitoned_clustered') }}
limit 100