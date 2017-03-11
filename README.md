# big-data-samples
This repository is a collecttion of different bidata work and project using both Map Reduce and Spark Approach

1-- weather.scala
 
 Processing a weather trend from an URL source giving Weather as a JSON structure text.
  The weather trend processing is done by Spark Scala using a task thread that fetch Json data  every minute from the Server URL    and process with Dataframe.
  - Run the script in the Spark-shell 
  Go to the directory were the scala file is saved run scala for spark in the prompt command
  >> cd /path.../weather.scala
  >> spark-shell
  In the scala CLI(Command line Interface) load the scala file by typing the command below 
   scala > :load weather.scala
   type crtl Z to exit the scala CLI

