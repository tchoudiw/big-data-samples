Setting Up a Sample Application in HBase, Spark, and HDFS 
This Article coming from https://dzone.com/articles/sample-app-hbase-spark-hdfs Show How to Use Hbase with Spark.
You can get Original code at https://gist.github.com/bolerio/f70c374c3e96495f5a69
HBase Install

We start with HBase because in fact you don't even need Hadoop to develop locally with it. There is of course a standard package for HBase and the latest stable release (at the time of this writing) is 1.1.2. But that's not going to do it for us because we want Spark. There is an integration of Spark with HBase that is being included as an official module in HBase, but only as of the latest 2.0.0 version, which is still in an unstable SNAPSHOT state. If you are just starting with these technologies you don't care about having a production blessed version. So, to install HBase, you need to build it first. Here are the steps:

    Clone the whole project from GIT with:
         git clone https://github.com/apache/hbase.git
    Make sure you have the latest Apache Maven (3.0.5 won't work),  get 3.3+, if you don't already have it.
    Go to the Hbase project directory and build it with:
         mvn -DskipTests=true installThat will put all hbase modules in your local maven repo, which you'll need for a local maven-based Spark project.
    Then create an hbase distribution with:
        mvn -DskipTests=true package assembly:single
    You will find tarball under hbase-assembly/target/hbase-2.0.0-SNAPSHOT-bin.tar.gz.
    This is what you are going to unpack in your installation directory of choice, say /opt/hbase.
    Great. Now you have to configure your new hbase installation (untar of the build you just created). Edit the file /opt/hbase/conf/hbase-site.xml to specify where Hbase should store its data. Use this template and replace the paths inside:

<configuration>

  <property>

    <name>hbase.rootdir</name>

    <value>file:///home/boris/temp/hbase</value>

  </property>

</configuration>


    Make sure you can start HBase by running:      bin/start-hbase.sh (or .cmd)
    Then start the HBase command line shell with:
    bin/hbase shell and type the command list to view the list of all available tables - there will be none of course.

Spark Development Setup

Spark itself is really the framework you use to do your data processing. It can run in a cluster environment, with a server to which you submit jobs from a client. But to start, you don't actually need any special downloads or deployments. Just including Maven dependencies in your project will bring the framework in, and you can call it from a main program and run it in a single process.

So assuming you built HBase as explained above, let's setup a Maven project with HBase and Spark. Then we'll get some sample data to play with and go over a sample application that makes use of two different approaches in Spark: the plain API and  the SQL-like Spark module which essentially gives a scalable, distributed SQL-query engine!

No point repeating the code in its entirety here, but I will show the relevant parts. The Maven pom only needs to contain the hbase-spark dependency:

<dependency>

    <groupId>org.apache.hbase</groupId>

    <artifactId>hbase-spark</artifactId>

    <version>${hbase.version}</version>

</dependency>

That will pull in everything you need to load that into a local HBase instance and process it with Spark. That Spark SQL module I mentioned is a separate dependency only necessary for the SQL portion of the example:

<dependency>

    <groupId>org.apache.spark</groupId>

    <artifactId>spark-sql_2.10</artifactId>

    <version>1.5.1</version>

</dependency>

There are two other auxiliary dependencies of the sample project: one for CSV parsing and the mJson library, which you can see in the pom.xml file from the Github GIST.
Playing With Some Data

We will do some processing now with some Open Government data, specifically from Miami-Dade County's list of recently arrested individuals. You can get it from here: https://opendata.miamidade.gov/Corrections/Jail-Bookings-May-29-2015-to-current/7nhc-4yqn - export in Excel CSV format as a file named jaildata.csv. I've already made the export and placed in the GIST even though that's a bit of a large file. The data set is simple: it contains arrests for a big part of the year 2015. Each record/line has the date the arrest was made, where it was made, the name of the offender, their date of birth and a list of up to 3 charges. We could for example find out how popular is the "battery" charge for offenders under 30 compared to offenders above 30.

First step is to import the data from the CSV file to HBase (note that Spark could work very well directly from a CSV file). This is done in the ImportData.java program where in a short main program we create a connection to HBase, create a brand new table called jaildata, then loop through all the rows in the CSV file to import the non-empty ones. I've annotated the source code directly. The connection assumes a local HBase server running on a default port and that the table doesn't exist yet. Note that data is inserted as a batch of put operations, one per column value. Each put operation specifies the column family, column qualifier and the value while the version is automatically assigned by the system. Perhaps the most important part in that uploading of data is how the row keys are constructed. HBase gives you complete freedom to create a unique key for each row inserted and it's sort of an artform to pick a good one. In our case, we chose the offender's name combined with the date of the arrest, the assumption being of course that the same person cannot be arrested twice in the same day (not a very solid assumption, of course).

Now we can show off some Spark in action. The file is ProcessData.java, which is also annotated, but I'll go over some parts here.

We have a few context objects and configuration objects that need to be initialized:

  private void init()

  {

      // Default HBase configuration for connecting to localhost on default port.

      conf = HBaseConfiguration.create();

      // Simple Spark configuration where everything runs in process using 2 worker threads.

      sparkconf = new SparkConf().setAppName("Jail With Spark").setMaster("local[2]");

      // The Java Spark context provides Java-friendly interface for working with Spark RDDs

      jsc = new JavaSparkContext(sparkconf);

      // The HBase context for Spark offers general purpose method for bulk read/write

      hbaseContext = new JavaHBaseContext(jsc, conf);

      // The entry point interface for the Spark SQL processing module. SQL-like data frames

      // can be created from it.

      sqlContext = new org.apache.spark.sql.SQLContext(jsc);

  }

A configuration object for HBase will tell the client where the server is etc., in our case default values for local server work. Next line, the Spark configuration gives it an application name and then it tells it where the main driver of the computation is - in our case, we have a local in-process driver that is allowed to use two concurrent threads. The Spark architecture comprises a main driver and then workers spread around in a cluster. A location of a remote main driver might look something like this: spark://masterhost:7077 (instead of "local[2]"). Then we create a Spark context, specifically a JavaSparkContext because the original Scala API is not so easy to work with from Java - possible, just not very user-friendly. Then we create something called a JavaHBaseContext which comes from the HBase-Spark module and it knows how talk to an HBase instances using the Spark data model - it can do bulk inserts, deletes, it can scan an HBase table as a Spark RDD and more. Finally, we create a context object representing an SQL layer on top of Spark data sets. Note that the SQL context object does not depend on HBase. In fact, different data sources can be brought under the same SQL processing API as long as there is some way to "relationalize" it. For example, there is a module that lets you process data coming from MongoDB as Spark SQL data as well. So in fact, you can have a federated data environment using a Spark cluster to perform relational joins between MongoDB collections and HBase table (and flat files and ...).    

Now, reading data from HBase is commonly done by scanning. You can perform some filtering operations, but there's no general purpose query language for it. That's the role of Spark and other frameworks like Apache Phoenix for example. Also, scanning HBase rows will give you binary values which need to be converted to the appropriate runtime Java type. So for each column value, you have to manage yourself what its type is and perform the conversion. The HBase API has a convenience class named Bytes that handles all basic Java types. To represent whole records, we use JSON as a data structure so individual column values are first converted to JSON values with this utility method:

  static final 

                Json value(Result result,  String family, String qualifier, BiFunction

                 converter)

  {

      Cell cell = result.getColumnLatestCell(Bytes.toBytes(family), Bytes.toBytes(qualifier));

      if (cell == null)

          return Json.nil();

      else

          return Json.make(converter.apply(cell.getValueArray(), cell.getValueOffset()));

  }

Given an HBase result row, we create a JSON object for our jail arrests records like this:

static final Function

                tojsonMap = (result) -> {

    Json data = Json.object()

        .set("name", value(result, "arrest", "name", Bytes::toString))

        .set("bookdate", value(result, "arrest", "bookdate", Bytes::toString))

        .set("dob", value(result, "arrest", "dob", Bytes::toString))

        .set("charge1", value(result, "charge", "charge1", Bytes::toString))

        .set("charge2", value(result, "charge", "charge2", Bytes::toString))

        .set("charge3", value(result, "charge", "charge3", Bytes::toString))

    ;

    return data;

};

With this mapping from row binary HBase to a runtime JSON structure we can construct a Spark RDD for the whole table as JSON records:

JavaRDD<Json> data = hbaseContext.hbaseRDD(TableName.valueOf("jaildata"), new Scan())

                                       .map(tuple -> tojsonMap.apply(tuple._2()));

We can then filter or transform that data anyway we want. For example:

data = data.filter(j -> j.at("name").asString().contains("John"));

would gives a new data set which contains only offenders named John. An instance of JavaRDD is really an abstract representation of a data set. When you invoke filtering or transformation methods on it, it will just produce an abstract representation of a new data set, but it won't actually compute the result. Only when you invoke what is called an action method, that has to return something different than an RDD as its value, the lazy computation is triggered and an actual data set will be produced. For instance, collect and count are such action methods.

Ok, good. Running ProcessData.main should output something like this:

Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties

15/12/29 00:41:00 INFO Remoting: Starting remoting

15/12/29 00:41:00 INFO Remoting: Remoting started; listening on addresses :[akka.tcp://sparkDriver@10.0.0.13:55405]

Older 'battery' offenders: 2548, younger 'battery' offenders: 1709, ratio(older/younger)=1.4909303686366295

Thefts: 34368

Contempts of court: 32

Hadoop

To conclude, I will just show you how to use Hadoop/HDFS to store HBase data instead of the normal OS filesystem. First, download Hadoop from https://hadoop.apache.org/releases.html. I used version 2.7.1. You can unzip the tarball/zip file in a standard location for your OS (e.g. /opt/hadoop on Linux).  Two configuration steps are important before you can actually start it:

    Point it to the JVM. It needs at least Java 7.  Edit etc/hadoop/hadoop-end.sh (or hadoop-env.cmd) and change the line export JAVA_HOME=${JAVA_HOME} to point the Java home you want, unless your OS/shell environment already does.
    Next you need to configure where Hadoop will store its data and what will be the URL for clients to access it. The URL for clients is configured in the etc/hadoop/core-site.xml file:

<configuration>

 <property>

  <name>fs.defaultFS</name>

  <value>hdfs://localhost:9000</value>

 </property>

</configuration>

The location for data is in the etc/hadoop/hdfs-site.xml file. And there are in fact two locations: one for the Hadoop Namenode and one for the Hadoop Datanode:

<configuration>

  <property>

    <name>dfs.namenode.name.dir</name>

    <value>file:///var/dfs/name</value>

  </property>

  <property>

    <name>dfs.datanode.data.dir</name>

    <value>file:///var/dfs/data</value>

  </property>

</configuration>

Before starting HDFS, you need to format the namenode (that's the piece the handles file names and directory names and knows what is where) by doing
      bin/hadoop namenode -format
When starting hadoop, you only need to start the hdfs file system by running
      sbin/start-dfs.shMake sure it has permissions to write to the directories you have configured.
If after hadoop has started, you don't see the admin web UI at http://localhost:50070, check the logs.
To connect HBase to hadoop you must change the hbase root directory to be an HDFS one:

  <property>

    <name>hbase.rootdir</name>

    <value>hdfs://localhost:9000/hbase</value>

  </property>

Restarting HBase now will bring you back with an empty database as you could verify on the hbase shell with the 'list' command. So try running the import program and then the "data crunching" program see what happens.
