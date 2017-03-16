add jar hive-serdes-1.0-SNAPSHOT.jar ;
--Drop all the tables 
DROP TABLE IF EXISTS land.weatherdata;
DROP TABLE IF EXISTS land.mystage;
DROP TABLE IF EXISTS expl.weatherdata;
DROP DATABASE land;
create database land;
DROP DATABASE expl;
create database expl;
CREATE EXTERNAL  TABLE IF NOT EXISTS land.weatherdata (
location struct<country: string,
lat: double,
localtime: string,
localtime_epoch: int,
lon: double,
name: string,
region: string,
tz_id: string>,
`current` STRUCT<cloud:int, condition : STRUCT<code:int, icon:STRING, text:STRING>, feelslike_c:double,feelslike_c: double, feelslike_f:double,humidity:int,is_day:int,
last_updated: string,
last_updated_epoch: int,
precip_in: double,
precip_mm: double,
pressure_in: double,
pressure_mb: double,
temp_c: double,
temp_f: double,
wind_degree: int,
wind_dir: string,
wind_kph: double,
wind_mph: double>)
ROW FORMAT SERDE "com.cloudera.hive.serde.JSONSerDe"
LOCATION "/user/hive/warehouse/weather";

-- Create staging table 

create  table land.mystage(location struct<country: string,
lat: double,
localtime: string,
localtime_epoch: int,
lon: double,
name: string,
region: string,
tz_id: string>,
`current` STRUCT<cloud:int, condition : STRUCT<code:int, icon:STRING, text:STRING>, feelslike_c:double,feelslike_c: double, feelslike_f:double,humidity:int,is_day:int,
last_updated: string,
last_updated_epoch: int,
precip_in: double,
precip_mm: double,
pressure_in: double,
pressure_mb: double,
temp_c: double,
temp_f: double,
wind_degree: int,
wind_dir: string,
wind_kph: double,
wind_mph: double>,
snapshot_time timestamp,
country string)
STORED AS TEXTFILE;


--Create managed tables expl.weather
create  table expl.weatherdata(location struct<country: string,
lat: double,
localtime: string,
localtime_epoch: int,
lon: double,
name: string,
region: string,
tz_id: string>,
`current` STRUCT<cloud:int, condition : STRUCT<code:int, icon:STRING, text:STRING>, feelslike_c:double,feelslike_c: double, feelslike_f:double,humidity:int,is_day:int,
last_updated: string,
last_updated_epoch: int,
precip_in: double,
precip_mm: double,
pressure_in: double,
pressure_mb: double,
temp_c: double,
temp_f: double,
wind_degree: int,
wind_dir: string,
wind_kph: double,
wind_mph: double>,
Snapshort_time timestamp)
partitioned by (country string)
row format delimited fields terminated by "|"
STORED AS TEXTFILE;






exit ;
