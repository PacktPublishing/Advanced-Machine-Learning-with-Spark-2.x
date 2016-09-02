# Apache Spark 2.0.0 application starter template

## Features

- Can use Spark interactively from the console:

```
$ sbt
...
[spark2-project1]> console
...
Welcome to Scala 2.11.8 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_45).
Type in expressions for evaluation. Or try :help.

scala> import org.apache.spark.sql.SparkSession; import org.apache.spark.SparkContext; import org.apache.spark.SparkContext._; import org.apache.spark.SparkConf; val conf = new SparkConf().setAppName("Simple Application").setMaster("local").set("spark.rpc.netty.dispatcher.numThreads","2"); val sc = new SparkContext(conf); 
16/09/02 14:53:41 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
conf: org.apache.spark.SparkConf = org.apache.spark.SparkConf@6c42f434
sc: org.apache.spark.SparkContext = org.apache.spark.SparkContext@1d63a678

scala> val logFile = "src/main/resources/log4j.properties"
logFile: String = src/main/resources/log4j.properties

scala> val logData = sc.textFile(logFile, 2).cache()
logData: org.apache.spark.rdd.RDD[String] = src/main/resources/log4j.properties MapPartitionsRDD[1] at textFile at <console>:19

scala> val numAs = logData.filter(line => line.contains("a")).count()
numAs: Long = 28

scala> val numBs = logData.filter(line => line.contains("b")).count()
numBs: Long = 7

scala> val spark = SparkSession.builder().appName("financial_data").master("local").getOrCreate()
16/09/02 14:55:10 WARN SparkContext: Use an existing SparkContext, some configuration may not take effect.
spark: org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSession@4f045932

scala> val opts = Map("url" -> "jdbc:postgresql:somedb", "dbtable" -> "sometableinthedb")
opts: scala.collection.immutable.Map[String,String] = Map(url -> jdbc:postgresql:somedb, dbtable -> sometableinthedb)

scala> val df = spark.read.format("jdbc").options(opts).load
df: org.apache.spark.sql.DataFrame = ...

scala> df.show(false)
...

scala> sc.stop()
```

and then back in the console (ctrl+d):

```
[spark2-project1]> run
...
[info] Running com.example.Hello
16/07/31 19:30:11 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Lines with a: 28, Lines with b: 7
[success] Total time: 4 s, completed Jul 31, 2016 7:30:13 PM
```

## References:

https://stackoverflow.com/questions/31685408/spark-actor-not-found-for-actorselection

https://jaceklaskowski.gitbooks.io/mastering-apache-spark/content/spark-logging.html

https://spark.apache.org/docs/latest/quick-start.html