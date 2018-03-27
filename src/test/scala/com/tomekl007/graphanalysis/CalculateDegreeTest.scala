package com.tomekl007.graphanalysis

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class CalculateDegreeTest extends FunSuite {
  val spark: SparkContext = SparkSession.builder().master("local[2]").getOrCreate().sparkContext
  /**
    * A common aggregation task is computing the degree of each vertex:
    * the number of edges adjacent to each vertex.
    * In the context of directed graphs it is often necessary to know the in-degree,
    * out-degree, and the total degree of each vertex.
    * The GraphOps class contains a collection of operators to compute the degrees of each vertex.
    * For example in the following we compute the max in, out, and total degrees:
    */
  test("should calculate degree of vertices") {
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
    val degrees = graph.degrees.collect().toList

    //then
    degrees should contain theSameElementsAs List(
      (4L, 1L),
      (2L, 2L),
      (1L, 2L),
      (3L, 1L)
    )
  }

  test("should calculate in-degree of vertices") {
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
    val degrees = graph.inDegrees.collect().toList

    //then
    degrees should contain theSameElementsAs List(
      (2L, 1L),
      (3L, 1L),
      (4L, 1L)
    )
  }

  test("should calculate out-degree of vertices") {
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
    val degrees = graph.outDegrees.collect().toList

    //then
    degrees should contain theSameElementsAs List(
      (1L, 2L),
      (2L, 1L)
    )
  }

}
