package sensor

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import sensor.HBaseSensorStream._

// schema for sensor data
case class Sensor(resid: String,
                  date: String,
                  time: String,
                  hz: Double,
                  disp: Double,
                  flo: Double,
                  sedPPM: Double,
                  psi: Double,
                  chlPPM: Double)

object Sensor extends Serializable {
  private val cfDataBytes = Bytes.toBytes("data")
  private val cfAlertBytes = Bytes.toBytes("alert")
  private val colHzBytes = Bytes.toBytes("hz")
  private val colDispBytes = Bytes.toBytes("disp")
  private val colFloBytes = Bytes.toBytes("flo")
  private val colSedBytes = Bytes.toBytes("sedPPM")
  private val colPsiBytes = Bytes.toBytes("psi")
  private val colChlBytes = Bytes.toBytes("chlPPM")

  // function to parse line of sensor data into Sensor class
  def parseSensor(str: String): Sensor = {
    val p = str.split(",")
    Sensor(p(0), p(1), p(2), p(3).toDouble, p(4).toDouble, p(5).toDouble, p(6).toDouble, p(7).toDouble, p(8).toDouble)
  }

  //  Convert a row of sensor object data to an HBase put object
  def convertToPut(sensor: Sensor): (ImmutableBytesWritable, Put) = {
    val rowkey: String = createRowCompositeKey(sensor)
    val put = new Put(Bytes.toBytes(rowkey))
    // add to column family data, column  data values to put object
    put.add(cfDataBytes, colHzBytes, Bytes.toBytes(sensor.hz))
    put.add(cfDataBytes, colDispBytes, Bytes.toBytes(sensor.disp))
    put.add(cfDataBytes, colFloBytes, Bytes.toBytes(sensor.flo))
    put.add(cfDataBytes, colSedBytes, Bytes.toBytes(sensor.sedPPM))
    put.add(cfDataBytes, colPsiBytes, Bytes.toBytes(sensor.psi))
    put.add(cfDataBytes, colChlBytes, Bytes.toBytes(sensor.chlPPM))
    (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)
  }

  // convert psi alert to an HBase put object
  def convertToPutAlert(sensor: Sensor): (ImmutableBytesWritable, Put) = {
    val key: String = createRowCompositeKey(sensor)
    val p = new Put(Bytes.toBytes(key))
    // add to column family alert, column psi data value to put object
    p.add(cfAlertBytes, colPsiBytes, Bytes.toBytes(sensor.psi))
    (new ImmutableBytesWritable(Bytes.toBytes(key)), p)
  }

  private def createRowCompositeKey(sensor: Sensor) = {
    val dateTime = sensor.date + " " + sensor.time
    // create a composite row key: sensorid_date time
    val rowkey = sensor.resid + "_" + dateTime
    rowkey
  }

}
