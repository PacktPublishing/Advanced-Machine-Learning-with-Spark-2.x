package com.tomekl007.anomalydetection

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object DetectingAnomaliesInGraph {

  val Friend = "friend"
  val Follower = "follower"

  type UserName = String
  type Relation = String

  def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local[2]").getOrCreate().sparkContext
    val graph = constructInputGraph(spark)

//    val usersWithoutAnyFriend = findUsersThatHasNoFriends(graph)
//    println(s"users that has no friends: $usersWithoutAnyFriend")

//    val usersWithSuspiciousNumberOfFriends = usersWithTooMuchFriends(graph, 5)
//    println(s"users with suspicious number of friends: $usersWithSuspiciousNumberOfFriends")
//
    val potentialStalkers = findPotentialStalkers(graph)
    print(s"potential stalkers: $potentialStalkers")

  }


  def constructInputGraph(spark: SparkContext): Graph[(UserName), Relation] = {

    val users: RDD[(VertexId, UserName)] =
      spark.parallelize(Array(
        (1L, "Cristian"),
        (2L, "Michael"),
        (3L, "Luis"),
        (4L, "Maria"),
        (5L, "Tom"),
        (6L, "Joseph"),
        (7L, "Harry"),
        (8L, "Wladimir"),
        (9L, "Sara"),
        (10L, "Gareth")
      ))

    val relationships: RDD[Edge[String]] =
      spark.parallelize(Array(
        Edge(1L, 2L, Friend),
        Edge(2L, 3L, Friend),
        Edge(4L, 5L, Friend),
        Edge(6L, 8L, Friend),
        Edge(6L, 7L, Friend),
        Edge(6L, 5L, Friend),
        Edge(6L, 4L, Friend),
        Edge(6L, 3L, Friend),
        Edge(6L, 2L, Friend),
        Edge(6L, 7L, Friend),
        Edge(2L, 4L, Follower),
        Edge(2L, 7L, Follower),
        Edge(1L, 5L, Follower)
      ))

    Graph(users, relationships)
  }

  def findPotentialStalkers(graph: Graph[UserName, Relation]) = {
    graph
      .edges
      .filter(_.attr == Follower)
      .map(_.srcId)
      .collect()
      .toList
      .distinct
  }

  def findUsersThatHasNoFriends(graph: Graph[(UserName), Relation]): List[VertexId] = {
    graph
      .connectedComponents
      .vertices
      .map(_.swap)
      .groupByKey()
      .filter { case (vertexId, iterable) => iterable.size == 1 }
      .map(_._1)
      .collect()
      .toList
  }


  def usersWithTooMuchFriends(graph: Graph[UserName, Relation],
                              maximumNumberOfAllowedFriends: Int): List[VertexId] = {

    graph
      .edges
      .filter(_.attr == Friend)
      .groupBy(_.srcId)
      .filter(_._2.size >= maximumNumberOfAllowedFriends)
      .map(_._1)
      .collect()
      .toList
  }
}
