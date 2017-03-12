import org.apache.spark._
import org.apache.spark.streaming._
import scala.util.matching.Regex
import java.util.regex.Pattern;
import java.util.regex.Matcher;

//Step 1. Look at sensor format and measurement types. The first cell in the notebook gives an example of the streaming measurements coming from the weather station:
 // 1419408015\t0R1,Dn=059D,Dm=066D,Dx=080D,Sn=8.5M,Sm=9.5M,Sx=10.3M\n",
 //1419408016\t0R1,Dn=059D,Dm=065D,Dx=078D,Sn=8.5M,Sm=9.5M,Sx=10.3M\n",
 // 1419408016\t0R2,Ta=13.9C,Ua=28.5P,Pa=889.9H\n",

//Step 2. Import and create streaming context. Next, we will import and create a new instance of Spark's StreamingContext:
//Similar to the SparkContext, the StreamingContext provides an interface to Spark's streaming capabilities. The argument sc is the SparkContext, and 1 specifies a batch interval of one second.
val ssc = new StreamingContext(sc, Seconds(1))


//Step 3. Create DStream of weather data. Let's open a connection to the streaming weather data:

//This create a new variable lines to be a Spark DStream that streams the lines of output from the weather station.

val lines = ssc.socketTextStream("rtd.hpwren.ucsd.edu", 9999)
//val lines = sc.textFile("/user/cloudera/test.txt") //for test purpose


//Step 4. Read measurement. Next, let's read the average wind speed from each line and store it in a new DStream vals:

//his line uses flatMap() to iterate over the lines DStream, and parse to get the average wind speed.

//Parse the data from the sourse by getting the  Wind direction average deg (Dm) and convert it to int for further usage
val vals = lines.flatMap(line =>if (Pattern.compile(".*Dm=([0-9]+).*").matcher(line).matches) {
		Array(Pattern.compile(".*Dm=([0-9]+).*").matcher(line).group(1).toInt);}
	else {Array(0);})


//vals.take(4).foreach(println) //Display the contain of Rdd for test purpose


//Step 5. Create sliding window of data. We can create a sliding window over the measurements by calling the window() method:

//This create a new DStream called window that combines the ten seconds worth of data and moves by five seconds.
val windowedStream = vals.window(Seconds(10))


//Step 6. Define and call analysis function. We would like to find the minimum and maximum values in our window. Let's define a function that prints these values for an RDD:

//This function first prints the entire contents of the RDD by calling the collect() method. This is done to demonstrate the sliding window and would not be practical if the RDD was containing a large amount of data. Next, we check if the size of the RDD is greater than zero before printing the maximum and minimum values. the function is anonymous function in this case


windowedStream.foreachRDD{rdd => println(rdd.collect());
	if (rdd.count()>0) {println( "Max is : "+ rdd.max() + "Min is :"+ rdd.min() );}}



//Step 7. Start the stream processing. We call start() on the StreamingContext to begin the processing:

ssc.start()
//The sliding window contains ten seconds worth of data and slides every five seconds. In the beginning, the number of values in the windows are increasing as the data accumulates, and after Window 3, the size stays (approximately) the same. Since the window slides half as often as the size of the window, the second half of a window becomes the first half of the next window. For example, the second half of Window 5 is 310, 321, 323, 325, 326, which becomes the first half of Window 6.


//When you are done call the stop() on the streamingContext
ssc.stop()




	
