-- Creating external table for fhv trip data referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `ny-taxi-de-zoomcamp.trips_data_all.external_fhv_tripdata`
OPTIONS (
  format = 'csv',
  uris = ['gs://dtc_data_lake_ny-taxi-de-zoomcamp/raw/fhv_tripdata/fhv_tripdata_2019*']
);

SELECT count(*) from ny-taxi-de-zoomcamp.trips_data_all.external_fhv_tripdata;

CREATE OR REPLACE TABLE ny-taxi-de-zoomcamp.trips_data_all.fhv_tripdata_partitioned_clustered
PARTITION BY DATE(pickup_datetime)
CLUSTER BY dispatching_base_num 
AS
SELECT * FROM ny-taxi-de-zoomcamp.trips_data_all.external_fhv_tripdata;

SELECT count(*) from ny-taxi-de-zoomcamp.trips_data_all.fhv_tripdata_partitioned_clustered;

SELECT COUNT(DISTINCT dispatching_base_num) from ny-taxi-de-zoomcamp.trips_data_all.fhv_tripdata_partitioned_clustered;

SELECT COUNT(*)
FROM `ny-taxi-de-zoomcamp.trips_data_all.fhv_tripdata_partitioned_clustered`
WHERE (CAST(pickup_datetime AS DATE) BETWEEN '2019-01-01' AND '2019-03-31') AND 
       dispatching_base_num IN ('B00987', 'B02060', 'B02279');