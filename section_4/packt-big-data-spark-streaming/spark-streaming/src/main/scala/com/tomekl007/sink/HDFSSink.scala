package com.tomekl007.sink

import java.time.ZonedDateTime

import com.tomekl007.avro.{PageViewWithViewCounter => PageViewWithViewCounterAvro}
import com.tomekl007.sparkstreaming.PageViewWithViewCounter
import org.apache.avro.Schema
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroKeyInputFormat, AvroKeyOutputFormat}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

class HDFSSink[T] {

  def write(ssc: StreamingContext,
            result: DStream[PageViewWithViewCounter]): Unit = {
    val path = s"hdfs://ads/bot_filtering/${ZonedDateTime.now().toInstant.getEpochSecond}"
    val stream = result.map(ToAvroMapper.mapPageView)


    stream.foreachRDD(rdd =>
      save[PageViewWithViewCounter](ssc.sparkContext,
        PageViewWithViewCounterAvro.SCHEMA$,
        rdd,
        path)

    )

  }

  def save[T](spark: SparkContext,
              schema: Schema,
              result: RDD[PageViewWithViewCounterAvro],
              path: String)
             (implicit classTag: ClassTag[T]): Unit = {
    val job = Job.getInstance(spark.hadoopConfiguration)
    AvroJob.setOutputKeySchema(job, schema)

    result.map(x => (new AvroKey(x), null))
      .saveAsNewAPIHadoopFile(path,
        classTag.runtimeClass,
        classOf[NullWritable],
        classOf[AvroKeyOutputFormat[T]],
        job.getConfiguration)


  }

  def load[T](spark: SparkContext,
              inputPath: String,
              schema: Schema)
             (implicit classTag: ClassTag[T],
              fileSystem: FileSystem): RDD[T] = {
    loadRdd[T](spark, inputPath, schema).getOrElse(spark.emptyRDD[T])
  }

  private def loadRdd[T](spark: SparkContext,
                         path: String,
                         schema: Schema)
                        (implicit classTag: ClassTag[T],
                         fileSystem: FileSystem): Option[RDD[T]] = {

    val job = Job.getInstance(spark.hadoopConfiguration)
    FileInputFormat.setInputPaths(job, path)
    AvroJob.setInputKeySchema(job, schema)

    val rdd = spark.newAPIHadoopRDD(job.getConfiguration,
      classOf[AvroKeyInputFormat[T]],
      classOf[AvroKey[T]],
      classOf[NullWritable]).map(_._1.datum())

    Some(rdd)

  }
}

object ToAvroMapper {
  def mapPageView(p: PageViewWithViewCounter): PageViewWithViewCounterAvro = {
    PageViewWithViewCounterAvro
      .newBuilder()
      .setPageViewId(p.pageViewId)
      .setEventTimestamp(p.eventTime.toInstant.getEpochSecond)
      .setUserId(p.userId)
      .setUrl(p.url)
      .setViewsCounter(p.viewsCounter)
      .build()
  }
}
