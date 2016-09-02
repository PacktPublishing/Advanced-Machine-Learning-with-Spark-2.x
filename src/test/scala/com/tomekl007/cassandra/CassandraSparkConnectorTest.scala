package com.tomekl007.cassandra

import com.tomekl007.cassandra.model.KV
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class CassandraSparkConnectorTest extends FunSuite {
  test("should write and load data to cassandra using spark connector") {
    //given
    val spark = SparkSession.builder().master("local[2]").getOrCreate()
    val connector = new CassandraSparkConnector()
    val listInput = List(
      KV("first", 100),
      KV("second", 200)
    )
    val input = spark.sparkContext.makeRDD(listInput)

    //when
    connector.writeValues(input)

    //and when
    val res = connector.readValuesKV(spark).collect().toList
    assert(contains(res, listInput))

  }

  def contains(res: List[KV], expected: List[KV]): Boolean = {
    expected.map(e => res.contains(e)).size == expected.size
  }

  test("should join data from two DataSets") {
    //given
    val spark = SparkSession.builder().master("local[2]").getOrCreate()
    val connector = new CassandraSparkConnector()

    //when
    val res = connector.joinDataFromTwoSources(spark)

    //then
    assert(res.collect().length > 0)
  }
  test("should join rdd with cassandra table") {
    //given
    val spark = SparkSession.builder().master("local[2]").getOrCreate()
    val connector = new CassandraSparkConnector()
    val listInput = List(
      KV("first", 100),
      KV("second", 200)
    )
    val input = spark.sparkContext.makeRDD(listInput)

    //when
    val res = connector.joinWithCassandraKVTAble(spark, input).collect().toList

    //then
    assert(res.nonEmpty)
  }

  test("should repartition RDD according to Cassandra partition scheme") {
    //given
    val spark = SparkSession.builder().master("local[2]").getOrCreate()
    val connector = new CassandraSparkConnector()
    val listInput = List(
      KV("first", 100),
      KV("second", 200)
    )
    val input = spark.sparkContext.makeRDD(listInput)

    //when
    assert(input.partitions.size < 10)
    assert(input.partitioner.isEmpty)
    val res = connector.repartitionByReplica(spark, input)

    //then
    assert(res.partitions.size == 10)
    assert(res.partitioner.isDefined)
  }

  test("should join rdd with cassandra table after repartition") {
    //given
    val spark = SparkSession.builder().master("local[2]").getOrCreate()
    val connector = new CassandraSparkConnector()
    val listInput = List(
      KV("first", 100),
      KV("second", 200)
    )
    val input = spark.sparkContext.makeRDD(listInput)

    //when
    val res = connector.joinWithCassandraKVTAble(spark, input).collect().toList

    //then
    assert(res.nonEmpty)
  }

}
