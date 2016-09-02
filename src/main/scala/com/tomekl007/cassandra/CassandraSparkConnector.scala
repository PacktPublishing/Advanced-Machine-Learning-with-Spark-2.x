package com.tomekl007.cassandra

import com.datastax.spark.connector._
import com.tomekl007.cassandra.model.{KV, UserData}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

class CassandraSparkConnector {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").getOrCreate()
    readValuesKV(spark)
  }

  def readValuesKV(session: SparkSession): RDD[KV] = {
    session.sparkContext
      .cassandraTable("test", "kv")
      .map(r => KV(r.get[String]("key"), r.get[Int]("value")))
  }

  def readValuesUser(session: SparkSession): RDD[UserData] = {
    session.sparkContext
      .cassandraTable("test", "userdata")
      .map(r => UserData(r.get[String]("key"), r.get[Int]("age")))
  }

  def writeValues(input: RDD[KV]): Unit = {
    input
      .saveToCassandra("test", "kv")
  }

  def joinDataFromTwoSources(sparkSession: SparkSession): DataFrame = {
    import sparkSession.sqlContext.implicits._
    val kvValues = readValuesKV(sparkSession).toDF()
    val userValues = readValuesUser(sparkSession).toDF()
    kvValues.join(userValues, List("key"))
  }

  def joinWithCassandraKVTAble(sparkSession: SparkSession, toJoin: RDD[KV]) = {
    toJoin.joinWithCassandraTable("test", "kv").on(SomeColumns("key"))
  }

  def joinWithCassandraKVTAbleWithRepartition(sparkSession: SparkSession, toJoin: RDD[KV]) = {
    repartitionByReplica(sparkSession, toJoin)
      .joinWithCassandraTable("test", "kv").on(SomeColumns("key"))
  }

  def repartitionByReplica(spark: SparkSession, toJoin: RDD[KV]): RDD[KV] = {
    toJoin.repartitionByCassandraReplica("test", "kv", partitionsPerHost = 10)
  }
}
