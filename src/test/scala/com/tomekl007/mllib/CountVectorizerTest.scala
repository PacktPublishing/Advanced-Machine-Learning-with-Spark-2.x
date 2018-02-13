package com.tomekl007.mllib

import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

/**
  * CountVectorizer and CountVectorizerModel aim to help convert a
  * collection of text documents to vectors of token counts.
  * When an a-priori dictionary is not available,
  * CountVectorizer can be used as an Estimator to extract the vocabulary,
  * and generates a CountVectorizerModel.
  * The model produces sparse representations for the documents over the vocabulary,
  * which can then be passed to other algorithms like LDA.
  */
class CountVectorizerTest extends FunSuite {
  private val spark = SparkSession.builder().master("local[2]").getOrCreate()
  /** input:
    * id | texts
    * ----|----------
    * 0  | Array("a", "b", "c")
    * 1  | Array("a", "b", "b", "c", "a")
    * each row in texts is a document of type Array[String].
    * Invoking fit of CountVectorizer produces a CountVectorizerModel
    * with vocabulary (a, b, c).
    * Then the output column “vector” after transformation contains:
    *
    *
    * output (Each vector represents the token counts of the document
    * over the vocabulary.):
    * id | texts                           | vector
    * ----|---------------------------------|---------------
    * 0  | Array("a", "b", "c")            | (3,[0,1,2],[1.0,1.0,1.0])
    * 1  | Array("a", "b", "b", "c", "a")  | (3,[0,1,2],[2.0,2.0,1.0])
    */

  test("count vectorizer test") {

    val df = spark.createDataFrame(Seq(
      (0, Array("a", "b", "c")),
      (1, Array("a", "b", "b", "c", "a"))
    )).toDF("id", "words")

    // fit a CountVectorizerModel from the corpus
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(3)
      .setMinDF(2)
      .fit(df)

    // alternatively, define CountVectorizerModel with a-priori vocabulary
    val cvm = new CountVectorizerModel(Array("a", "b", "c"))
      .setInputCol("words")
      .setOutputCol("features")

    cvModel.transform(df).show(false)
  }

}
