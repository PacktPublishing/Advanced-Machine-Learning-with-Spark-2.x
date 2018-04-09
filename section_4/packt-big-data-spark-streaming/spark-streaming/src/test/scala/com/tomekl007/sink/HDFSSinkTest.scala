package com.tomekl007.sink

import com.tomekl007.avro.PageViewWithViewCounter
import com.tomekl007.{HdfsTestFileSystem, SparkSuite}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.scalatest.BeforeAndAfter
import org.scalatest.Matchers._

class HDFSSinkTest extends SparkSuite with BeforeAndAfter with HdfsTestFileSystem{
  override def appName = "sink-hdfs-test"
  after {
    cleanupFs()
  }

  test("saves and loads data") {
    implicit def fileSystem: FileSystem = FileSystem.getLocal(new Configuration())

    val pv = List(
      PageViewWithViewCounter.newBuilder()
        .setUserId("uid1")
        .setPageViewId(1)
        .setEventTimestamp(12345)
        .setUserId("1")
        .setUrl("www.a.com")
        .setViewsCounter(12)
        .build())
    val rdd = spark.parallelize(pv)

    new HDFSSink().save(spark, PageViewWithViewCounter.SCHEMA$, rdd, testPath("test"))

  }
}
