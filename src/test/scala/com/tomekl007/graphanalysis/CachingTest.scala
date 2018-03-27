package com.tomekl007.graphanalysis

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.scalatest.FunSuite

class CachingTest extends FunSuite {
  val spark: SparkContext = SparkSession.builder().master("local[2]").getOrCreate().sparkContext

  /**
    * In Spark, RDDs are not persisted in memory by default.
    * To avoid recomputation, they must be explicitly cached when
    * using them multiple times (see the Spark Programming Guide).
    * Graphs in GraphX behave the same way.
    * When using a graph multiple times, make sure to call Graph.cache() on it first.
    */
  test("when not caching intermediate result it will take longer time") {
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
    val res = graph.mapEdges(e => e.attr.toUpperCase)
    val res2 = graph.mapVertices((_, att) => att.toUpperCase()) //it will load graph again

    res.edges.collect().toList
    res2.edges.collect().toList
  }

  test("when use caching on intermediate result will be calculated faster") {
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

    val graph = Graph(users, relationships).cache()

    //when
    val res = graph.mapEdges(e => e.attr.toUpperCase).cache()
    val res2 = graph.mapVertices((_, att) => att.toUpperCase()) //it will load graph again


    res.edges.collect().toList
    res2.edges.collect().toList
  }


}
