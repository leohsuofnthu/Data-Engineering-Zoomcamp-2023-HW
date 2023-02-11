-- Create External Table from FHV data on GitHub
CREATE OR REPLACE EXTERNAL TABLE `trips_data_all.fhv_tripdata_2019`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_dataengineeringzoocamp/fhv/fhv_tripdata_2019-*.csv.gz']
)

-- Q1 (Check number of rows)
SELECT COUNT(*) FROM `dataengineeringzoocamp.trips_data_all.fhv_tripdata_2019` 

-- Q2 (Check distinct Affiliated_base_number)
SELECT DISTINCT(Affiliated_base_number) FROM `trips_data_all.fhv_tripdata_2019` 
SELECT DISTINCT(Affiliated_base_number) FROM `trips_data_all.fhv_tripdata_2019_bq` 

-- Q3 (Check number of rows where both PUlocationID and DOlocationID are NULL)
SELECT COUNT(*) FROM `dataengineeringzoocamp.trips_data_all.fhv_tripdata_2019` 
WHERE PUlocationID  IS NULL AND DOlocationID IS NULL

-- Q4 (Create a table partitioned by pickup_datetime & clustered on affiliated_base_number)
CREATE OR REPLACE TABLE trips_data_all.fhv_tripdata_2019_partitioned_clustered
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number AS
SELECT * FROM trips_data_all.fhv_tripdata_2019_bq

-- Q5
SELECT DISTINCT(Affiliated_base_number) FROM `trips_data_all.fhv_tripdata_2019_bq` WHERE DATE(pickup_datetime) between '2019-03-01' and '2019-03-31'
SELECT DISTINCT(Affiliated_base_number) FROM `trips_data_all.fhv_tripdata_2019_partitioned_clustered` WHERE DATE(pickup_datetime) between '2019-03-01' and '2019-03-31'
