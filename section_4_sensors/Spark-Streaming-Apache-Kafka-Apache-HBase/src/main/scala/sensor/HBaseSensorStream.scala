/*
 *
 *
 */

package sensor

import hbase.HBaseConnector
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object HBaseSensorStream extends Serializable {
  final val tableName = "sensors"

  def main(args: Array[String]): Unit = {
    // set up HBase Table configuration
    val hbase = HBaseConnector.connect()
    hbase.conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val jobConfig: JobConf = new JobConf(hbase.conf, this.getClass)
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val sparkConf = new SparkConf()
      .setAppName("HBaseStream")
      .setMaster("local[*]")
    // create a StreamingContext, the main entry point for all streaming functionality
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // parse the lines of data into sensor objects
    val sensorDStream = ssc
      .textFileStream(s"${System.getProperty("user.dir")}/sensor")
      .map(Sensor.parseSensor)

    sensorDStream.print()

    sensorDStream.foreachRDD { rdd =>
      // filter sensor data for low psi
      val alertRDD = rdd.filter(sensor => sensor.psi < 5.0)
      alertRDD.take(1).foreach(println)
      // convert sensor data to put object and write to HBase table column family data
      rdd.map(Sensor.convertToPut).
        saveAsHadoopDataset(jobConfig)
      // convert alert data to put object and write to HBase table column family alert
      alertRDD.map(Sensor.convertToPutAlert).
        saveAsHadoopDataset(jobConfig)
    }
    // Start the computation
    ssc.start()
    // Wait for the computation to terminate
    ssc.awaitTermination()

  }

}