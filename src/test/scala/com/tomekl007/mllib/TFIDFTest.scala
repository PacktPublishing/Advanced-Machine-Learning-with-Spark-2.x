package com.tomekl007.mllib

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

/**
  * Term frequency-inverse document frequency (TF-IDF) is a feature
  * vectorization method widely used in text mining to reflect the
  * importance of a term to a document in the corpus.
  */
class TFIDFTest extends FunSuite {
  private val spark = SparkSession.builder().master("local[2]").getOrCreate()

  /**
    * In the following code segment, we start with a set of sentences.
    * We split each sentence into words using Tokenizer.
    * For each sentence (bag of words),
    * we use HashingTF to hash the sentence into a feature vector.
    * We use IDF to rescale the feature vectors;
    * this generally improves performance when using text as features.
    * Our feature vectors could then be passed to a learning algorithm.
    */
  test("should use TF_IDF ") {
    import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}

    val sentenceData = spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)
    wordsData.show()

    val hashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("rawFeatures")
      .setNumFeatures(20)

    val featurizedData = hashingTF.transform(wordsData)
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.show()
    rescaledData.select("label", "features").show()
  }

}
