package com.tomekl007.graphanalysis

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class VertexAndEdgeRDDTest extends FunSuite {
  private val spark = SparkSession.builder().master("local[2]").getOrCreate().sparkContext

  test("should take edges as an RDD") {
    //given
    val users: RDD[(VertexId, (String))] =
      spark.parallelize(Array(
        (1L, "a"),
        (2L, "b"),
        (3L, "c"),
        (4L, "d")
      ))


    val relationships =
      spark.parallelize(Array(
        Edge(1L, 2L, "friend"),
        Edge(1L, 3L, "friend"),
        Edge(2L, 4L, "wife")
      ))

    val graph = Graph(users, relationships)

    //when
    val edgesRDD = graph.edges

    //all RDD API operators available
    val res = edgesRDD.map(e => e.attr.toUpperCase())
        .filter(a => a != "FRIEND")
      .collect().toList
    res should contain theSameElementsAs List(
      "WIFE"
    )
  }

  test("should take verices as an RDD") {
    //given
    val users: RDD[(VertexId, (String))] =
      spark.parallelize(Array(
        (1L, "a"),
        (2L, "b"),
        (3L, "c"),
        (4L, "d")
      ))


    val relationships =
      spark.parallelize(Array(
        Edge(1L, 2L, "friend"),
        Edge(1L, 3L, "friend"),
        Edge(2L, 4L, "wife")
      ))

    val graph = Graph(users, relationships)

    //when
    val edgesRDD = graph.vertices

    //all RDD API operators available
    val res = edgesRDD.map(v => v._2.toUpperCase()).collect().toList
    res should contain theSameElementsAs List(
      "A", "B", "C", "D"
    )


  }


}
