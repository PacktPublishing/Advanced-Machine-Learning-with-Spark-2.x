/*
 * This example reads a row of time series sensor data
 * calculates the the statistics for the hz data
 * and then writes these statistics to the stats column family
 *
 */

package sensor

import hbase.HBaseConnector
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.util.StatCounter
import org.apache.spark.{SparkConf, SparkContext}

object HBaseReadWrite extends Serializable {

  final val tableName = "sensors"
  final val cfDataBytes = Bytes.toBytes("data")
  final val cfStatsBytes = Bytes.toBytes("stats")

  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
      .setAppName("HBaseTest")
      .setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val hbase = HBaseConnector.connect()
    hbase.conf.set(TableInputFormat.INPUT_TABLE, tableName)
    hbase.conf.set(TableInputFormat.SCAN_ROW_START, "COHUTTA_3/10/14")
    hbase.conf.set(TableInputFormat.SCAN_ROW_STOP, "COHUTTA_3/11/14")
    // specify specific column to return
    hbase.conf.set(TableInputFormat.SCAN_COLUMNS, "data:psi")

    // Load an RDD of (ImmutableBytesWritable, Result) tuples from the table
    val hBaseRDD = sc.newAPIHadoopRDD(
      hbase.conf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]
    )

    println(s"number of records fetched: ${hBaseRDD.count()}")

    // transform (ImmutableBytesWritable, Result) tuples into an RDD of Result’s
    val resultRDD = hBaseRDD.map(tuple => tuple._2)
    resultRDD.count()
    // transform into an RDD of (RowKey, ColumnValue)s  the RowKey has the time removed
    val keyValueRDD = resultRDD.map(result => (
      Bytes.toString(result.getRow()).split(" ")(0),
      Bytes.toDouble(result.value))
    )
    keyValueRDD.take(3).foreach(kv => println(kv))

    // group by rowkey , get statistics for column value
    val keyStatsRDD = keyValueRDD.groupByKey().mapValues(list => StatCounter(list))
    keyStatsRDD.take(5).foreach(println)

    // set JobConfiguration variables for writing to HBase
    val jobConfig: JobConf = new JobConf(hbase.conf, this.getClass)
    hbase.conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    // convert rowkey, psi stats to put and write to hbase table stats column family
    keyStatsRDD.map { case (k, v) => convertToPut(k, v) }.saveAsHadoopDataset(jobConfig)
    //
    //(COHUTTA_3/10/14,(count: 958, mean: 87.586639, stdev: 7.309181, max: 100.000000, min: 75.000000))
  }

  // convert rowkey, stats to put
  def convertToPut(key: String, stats: StatCounter): (ImmutableBytesWritable, Put) = {
    val p = new Put(Bytes.toBytes(key))
    // add columns with data values to put
    p.add(cfStatsBytes, Bytes.toBytes("psimax"), Bytes.toBytes(stats.max))
    p.add(cfStatsBytes, Bytes.toBytes("psimin"), Bytes.toBytes(stats.min))
    p.add(cfStatsBytes, Bytes.toBytes("psimean"), Bytes.toBytes(stats.mean))
    (new ImmutableBytesWritable, p)
  }

}
