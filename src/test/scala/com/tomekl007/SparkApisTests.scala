package com.tomekl007

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class SparkApisTests extends FunSuite {
  val spark = SparkSession.builder().master("local[2]").getOrCreate()

  test("rdd") {
    //given
    val rdd: RDD[String] =
      spark.sparkContext.parallelize(Array(
        "a", "b", "c", "d"
      ))

    //when
    val res = rdd.map(_.toUpperCase).collect().toList

    res should contain theSameElementsAs List(
      "A", "B", "C", "D"
    )
  }

  test("dataFrame") {
    import spark.sqlContext.implicits._
    val userData =
      spark.sparkContext.makeRDD(List(
        UserData("a", "1"),
        UserData("b", "2")
      )).toDF()
    userData.createOrReplaceTempView("user_data")

    //when
    val userDataForUserIdA = userData
      .where("userId = 'a'")
      .count()


    //then
    assert(userDataForUserIdA == 1)
  }


  test("dataSet aka typed DataFrame") {
    import spark.sqlContext.implicits._
    val userData =
      spark.sparkContext.makeRDD(List(
        UserData("a", "1"),
        UserData("b", "2")
      )).toDS()
    userData.createOrReplaceTempView("user_data")

    //when
    val userDataForUserIdA = userData
      .filter(_.userId == "a")
      .count()


    //then
    assert(userDataForUserIdA == 1)
  }

}

case class UserData(userId: String, data: String)

