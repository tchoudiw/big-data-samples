//This is the trend weather treatment directly in spark using the URL source json of one city
import java.io._
import sys.process._
import java.net.URL

//val path = "/home/cloudera/Downloads/test.txt"
//new File(path).delete()
def weatherTrend( ) : Unit = {
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
	// Displays the content of the DataFrame to stdout
	//df.show()
	//df.printSchema()
	new URL("http://api.apixu.com/v1/current.json?key=a14ffe7f0f9b464bbe023057172901&q=Toronto") #> new File("/home/cloudera/Downloads/weather.json") !! // save data in the weather.json file
    	val df = sqlContext.read.json("file:/home/cloudera/Downloads/weather.json") //convert in dataframe
	df.registerTempTable("weather") //register the json file as a table in memory
	val weatherall = sqlContext.sql("SELECT location.country, location.name, location.localtime, current.temp_c, current.temp_f, current.humidity FROM 	weather").show() //query and diplay result of weather 
   }
weatherTrend( )
// ===== Play trend weather by downloading and display the selected weather data at a regular interval of time in a thread =========
val trend = new java.util.Timer()
val task = new java.util.TimerTask {
  def run() = weatherTrend( )
}
trend.schedule(task, 1000L, 60000L) // process every 1 minute
task.cancel() //call it to cancell the task 
