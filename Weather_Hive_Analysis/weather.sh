#!/bin/sh
#Author William Tchoudi
#List of 10 cities from different countries to loop on and get weather data 
cities="Paris Toronto Douala Madrid Legos Bamako Dakar Tunis Alger Dallas" 

# if the directory exist in HDFS delete and create new one 
hadoop fs -rm -R /user/hive/warehouse/weather
hadoop fs -rm -R /user/hive/warehouse/weather_logs
hadoop fs -mkdir /user/hive/warehouse/weather_logs # To archieve data which are alresdy loaded in internal table

#Tables creation of land.weatherdata table, a staging table named land.mystage and the magaged table expl.weatherdata
# download the hive-serdes-1.0-SNAPSHOT.jar from https://bigdatadevelop.wordpress.com/2015/01/02/working-with-json-files-in-hive/
# add the jar to Hive (for some reason doesn't work properly all the time, use "add jar" statement below to make sure JSON based operations work properly)

hive -f /home/cloudera/Downloads/createTable.hql

#Loop over 10 cities and store data into HDFS land Area
for city in $cities #get the weather for 10 countries
do
	wget -O $city'.json' 'http://api.apixu.com/v1/current.json?key=a14ffe7f0f9b464bbe023057172901&q='$city #download data in landing area
	#hadoop fs -rm /user/hive/warehouse/weather/$city'.json' 
	hadoop fs -put $city'.json' /user/hive/warehouse/weather
	
done
# Load data from landing table land.weatherdata into staging table land.mystaging 
hive -f /home/cloudera/Downloads/insertTable.hql

# archive data from previous run
hadoop fs -mv /user/hive/warehouse/weather/* /user/hive/warehouse/weather_logs

#Delete json file on edgenode 
for city in $cities #get the weather for 10 countries
do
	rm /home/cloudera/Downloads/$city'.json' # delete file after copying it to HDFS for archiving
done

# Some querries for weather information and trend Analysis

hive -f /home/cloudera/Downloads/weatherAnalysis.hql


