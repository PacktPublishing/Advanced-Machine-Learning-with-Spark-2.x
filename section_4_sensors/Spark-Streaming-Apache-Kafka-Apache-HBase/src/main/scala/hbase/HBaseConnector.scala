package hbase


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, MasterNotRunningException}

class HBaseConnector(val conf: Configuration)


object HBaseConnector {
  def connect(): HBaseConnector = {
    val config = HBaseConfiguration.create
    val path = this.getClass.getClassLoader.getResource("hbase-site.xml").getPath

    config.addResource(new Path(path))

    try
      HBaseAdmin.checkHBaseAvailable(config)
    catch {
      case e: MasterNotRunningException =>
        throw new RuntimeException("HBase is not running", e)
    }
    new HBaseConnector(config)
  }
}