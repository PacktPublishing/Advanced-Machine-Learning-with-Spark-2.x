package com.tomekl007.mllib

import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

/**
  * Latent Dirichlet allocation (LDA) is a topic model which infers
  * topics from a collection of text documents.
  * LDA can be thought of as a clustering algorithm as follows:
  * *
  * Topics correspond to cluster centers,
  * and documents correspond to examples (rows) in a dataset.
  * Topics and documents both exist in a feature space,
  * where feature vectors are vectors of word counts (bag of words).
  * Rather than estimating a clustering using a
  * traditional distance, LDA uses a function based on a
  * statistical model of how text documents are generated.
  */
class LDATest extends FunSuite {
  private val spark = SparkSession.builder().master("local[2]").getOrCreate()

  test("should use LDA to find topics") {
    // Load and parse the data
    val data = spark.sparkContext.textFile("data/mllib/sample_lda_data.txt")
    val parsedData = data.map(s => Vectors.dense(s.trim.split(' ').map(_.toDouble)))
    // Index documents with unique IDs
    val corpus = parsedData.zipWithIndex.map(_.swap).cache()

    // Cluster the documents into three topics using LDA
    val ldaModel = new LDA().setK(3).run(corpus)

    // Output topics. Each is a distribution over words (matching word count vectors)
    println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")
    val topics = ldaModel.topicsMatrix
    for (topic <- Range(0, 3)) {
      print("Topic " + topic + ":")
      for (word <- Range(0, ldaModel.vocabSize)) {
        print(" " + topics(word, topic))
      }
      println()
    }

  }

}
