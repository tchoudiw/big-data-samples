add jar hive-serdes-1.0-SNAPSHOT.jar ;
-- insert from land table to staging table 
insert into land.mystage
select location, `current`, FROM_UNIXTIME( UNIX_TIMESTAMP()), location.country
from land.weatherdata;

--Insert from staging table to exploratory table 
set hive.exec.dynamic.partition.mode=nonstrict;
insert into expl.weatherdata
partition (country) 
select DISTINCT f.location, f.`current`,f.snapshot_time, f.country
from land.mystage f left join expl.weatherdata t on (f.country = t.country and f.`current`.last_updated = t.`current`.last_updated)
where t.country is null;
