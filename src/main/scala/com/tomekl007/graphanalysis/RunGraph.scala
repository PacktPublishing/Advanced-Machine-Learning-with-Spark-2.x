package com.tomekl007.graphanalysis

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object RunGraph {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").getOrCreate().sparkContext
    print("simpleOperations:")
    simpleGraphOperations(spark)
//    print("structuralOperations:")
//    structuralOperations(spark)
////    print("neighborhoodAggregation:")
////    neighborhoodAggregation(spark)

  }

  private def simpleGraphOperations(spark: SparkContext) = {

    //vertices
    val users: RDD[(VertexId, (String, String))] =
      spark.parallelize(Array(
        (3L, ("rxin", "student")),
        (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")),
        (2L, ("istoica", "prof")))

      )

    //edges
    val relationships: RDD[Edge[String]] =
      spark.parallelize(Array(
        Edge(3L, 7L, "collab"), //[3L] -> collab -> [7l]
        Edge(5L, 3L, "advisor"), //5l -> advisor -> 3L
        Edge(2L, 5L, "colleague"), //2L -> college -> 5L
        Edge(5L, 7L, "collegue")) // 5l; ->>
      )


    // Build the initial Graph
    val graph: Graph[(String, String), String] = Graph(users, relationships)

    // Count all users which are postdocs
    val countOfPostdoc = graph.vertices.filter {
      case (id, (name, pos)) => pos == "postdoc" }.count
    println(s"countOfPostdocs: $countOfPostdoc")

    // Count all the edges where src > dst
    val c = graph.edges.filter(e => e.srcId > e.dstId).count
    println(s"countOfSrc>Dst: $c")

    graph.edges.filter { case Edge(src, dst, prop) => src > dst }.count

    // Use the triplets view to create an RDD of facts.
    val facts: RDD[String] =
      graph.triplets.map(triplet =>
        triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
    facts.collect.foreach(println(_))
  }


  def structuralOperations(sc: SparkContext): Unit = {
    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array(
        (3L, ("rxin", "student")),
        (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")),
        (2L, ("istoica", "prof")),
        (4L, ("peter", "student")))
      )
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(
        Edge(3L, 7L, "collab"),
        Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"),
        Edge(5L, 7L, "pi"),
        Edge(4L, 0L, "student"),
        Edge(5L, 0L, "colleague"))
      )
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)
    // Notice that there is a user 0 (for which we have no information) connected to users
    // 4 (peter) and 5 (franklin).

    connectedComponents(graph)
  }


  private def connectedComponents(graph: Graph[(String, String), String]) = {
    // Run Connected Components
    val ccGraph = graph.connectedComponents() // No longer contains missing field
    ccGraph.triplets.map(
      triplet => triplet.srcAttr + " is the " + triplet.attr + " of " + triplet.dstAttr
    ).collect.foreach(println(_))
  }

  private def subgraph(graph: Graph[(String, String), String]) = {
    graph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))
    // Remove missing vertices as well as the edges to connected to them
    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    // The valid subgraph will disconnect users 4 and 5 by removing user 0
    validGraph.vertices.collect.foreach(println(_))
    validGraph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))
  }

  def neighborhoodAggregation(sc: SparkContext): Unit = {
    // Create a graph with "age" as the vertex property.
    // Here we use a random graph for simplicity.
    val graph: Graph[Double, Int] =
    GraphGenerators.logNormalGraph(sc, numVertices = 100).mapVertices((id, _) => id.toDouble)
    // Compute the number of older followers and their total age
    val olderFollowers: VertexRDD[(Int, Double)] = graph.aggregateMessages[(Int, Double)](
      triplet => { // Map Function
        if (triplet.srcAttr > triplet.dstAttr) {
          // Send message to destination vertex containing counter and age
          triplet.sendToDst(1, triplet.srcAttr)
        }
      },
      // Add counter and age
      (a, b) => (a._1 + b._1, a._2 + b._2) // Reduce Function
    )
    // Divide total age by number of older followers to get average age of older followers
    val avgAgeOfOlderFollowers: VertexRDD[Double] =
      olderFollowers.mapValues((id, value) =>
        value match {
          case (count, totalAge) => totalAge / count
        })
    // Display the results
    avgAgeOfOlderFollowers.collect.foreach(println(_))
  }
}