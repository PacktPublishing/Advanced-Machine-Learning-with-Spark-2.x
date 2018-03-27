# Advanced Machine Learning with Spark 2.x [video]
This is the code repository for [Advanced Machine Learning with Spark 2.x [video]](https://www.packtpub.com/big-data-and-business-intelligence/advanced-machine-learning-spark-2x-video?utm_source=github&utm_medium=repository&utm_campaign=9781788628242), published by [Packt](https://www.packtpub.com/?utm_source=github). It contains all the supporting project files necessary to work through the video course from start to finish.
## About the Video Course
The aim of this course is to provide practical understanding of advanced machine learning algorithms in Apache Spark for making predictions, recommendation and deriving insights among others from large distributed datasets. This course starts with an introduction of key concepts and datatypes that are fundamental to understanding distributed data processing and machine learning with spark. Further to this, we provide practical recipes that demonstrate some of the most popular algorithms in spark, leading to the creation of sophisticated machine learning pipelines and applications. The final sections are dedicated to more advanced use cases of machine learning namely Streaming, Natural Language Processing and Deep Learning. In each section we briefly establish the theoretical bases of the topic under discussion and then cement our understanding with practical use-cases. 

<H2>What You Will Learn</H2>
<DIV class=book-info-will-learn-text>
<UL>
<LI>Build efficient architecture for convolutional neural networks 
<LI>Construct a residual learning neural network for image recognition 
<LI>Build depthwise separable convolutional neural networks 
<LI>Construct conditional Generative Adversarial Networks (GAN) 
<LI>Build an advanced and powerful multi-class image classifier 
<LI>Build functional model class and methods with TensorFlow-Keras’ Functional API 
<LI>Build a computational graph representation of a Neural Network from state-of-the-art deep learning papers 
<LI>Optimize a neural network with stochastic gradient descent and other advanced optimization methods </LI></UL></DIV>

## Instructions and Navigation
### Assumed Knowledge
To fully benefit from the coverage included in this course, you will need:<br/>
To fully benefit from the coverage included in this course, you will need:
● Prior working knowledge of the Scala language
● Familiarity with Git and GitHub for source control
● Basic Understanding of Apache Spark technology

### Technical Requirements
This course has the following software requirements:<br/>
This course has the following software requirements:
● An IntelliJ IDE
● JDK 8 

This course has been tested on the following system configuration:
● OS: MAC OS Sierra
● Processor: Intel i7 2800
● Memory: 16GB

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


## Related Products
* [Learn Algorithms and Data Structures in Java for Day-to-Day Applications [Video]](https://www.packtpub.com/application-development/learn-algorithms-and-data-structures-java-day-day-applications-video?utm_source=github&utm_medium=repository&utm_campaign=9781788624428)

* [Mastering Ansible [Video]](https://www.packtpub.com/virtualization-and-cloud/mastering-ansible-video?utm_source=github&utm_medium=repository&utm_campaign=9781788629515)

* [Advanced Computer Vision with TensorFlow [Video]](https://www.packtpub.com/application-development/advanced-computer-vision-tensorflow-video?utm_source=github&utm_medium=repository&utm_campaign=9781788479448)

