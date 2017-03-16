1. Weather trend using Hive 


This Analysis is bases On getting data from weather URL source and process it Using Hive via a shell script(weather.sh)
The Hadoop distrubution used is Cloudra
Source URL 
http://api.apixu.com/v1/current.json?key=a14ffe7f0f9b464bbe023057172901&q=Paris

The Anaysis done follow the steps bellow
Use above web services to get current weather data as JSON. Parse file and store in hive table. 

#Follow conditions below

1. Download JSON file in Landing area in edge node
2. Transfer file to directory in HDFS /user/hive/warehouse/weather/
3. Create external table “land.weatherdata” in landing area to read data from /user/hive/warehouse/weather/.
4. Create staging table "land.mystage" to store Json string
5. Create “expl.weatherdata” internal table with partition on “country”
6. Confirm no duplicate data will be in Explorer area table
7. Add column “Snapshort_time”  to record data receiving time
8. After loading data to Explorer area move downloaded file to HDFS directory /user/hive/warehouse/weather_logs for archiving. And to confirm landing area will have only last downloaded file
9. Write shell script to
10. Get 10 countries weather data 
11. Calling shell script to run the process
12. Automated your process to receive and ingest data every 10 min interval in this case using cron job (Can be done late using OOZIE).

 schedule the task (I used cron, will try Oozie later)
 the following line should be copied into file /etc/crontab

10 * * * * /home/cloudera/Downloads ./weather1.sh

 to edit the file execute the following command

sudo vi cronjob10.sh

 to check if cron daemon runs in your VM, run the following in bash:

/sbin/service crond status

 you can start it if it is not running by the following command

/sbin/service crond start


13. Write Query to display current weather
14. Write query to find out weather changing trend

run the programm with the command on Linux
./weather.sh


