package com.tomekl007.anomalydetection


import java.io.File

import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Random

object RunKMeans {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").getOrCreate()

    val data = spark.read.
      option("inferSchema", true).
      option("header", false).
      csv(new File("resources/kddcup_truncated.data").getAbsolutePath).
      toDF(
        "duration", "protocol_type", "service", "flag",
        "src_bytes", "dst_bytes", "land", "wrong_fragment", "urgent",
        "hot", "num_failed_logins", "logged_in", "num_compromised",
        "root_shell", "su_attempted", "num_root", "num_file_creations",
        "num_shells", "num_access_files", "num_outbound_cmds",
        "is_host_login", "is_guest_login", "count", "srv_count",
        "serror_rate", "srv_serror_rate", "rerror_rate", "srv_rerror_rate",
        "same_srv_rate", "diff_srv_rate", "srv_diff_host_rate",
        "dst_host_count", "dst_host_srv_count",
        "dst_host_same_srv_rate", "dst_host_diff_srv_rate",
        "dst_host_same_src_port_rate", "dst_host_srv_diff_host_rate",
        "dst_host_serror_rate", "dst_host_srv_serror_rate",
        "dst_host_rerror_rate", "dst_host_srv_rerror_rate",
        "label")

    data.cache()

    val runKMeans = new RunKMeans(spark)

    runKMeans.buildAnomalyDetector(data)

    data.unpersist()
  }

}

class RunKMeans(private val spark: SparkSession) {

  import spark.implicits._


  def transformColumnToVectorOfFeatures(inputCol: String): (Pipeline, String) = {
    val indexer = new StringIndexer().
      setInputCol(inputCol).
      setOutputCol(inputCol + "_indexed")
    val encoder = new OneHotEncoder().
      setInputCol(inputCol + "_indexed").
      setOutputCol(inputCol + "_vec")
    val pipeline = new Pipeline().setStages(Array(indexer, encoder))
    (pipeline, inputCol + "_vec")
  }

  def buildMlPipeline(data: DataFrame, k: Int): PipelineModel = {
    val (protoTypeEncoder, protoTypeVecCol) = transformColumnToVectorOfFeatures("protocol_type")
    val (serviceEncoder, serviceVecCol) = transformColumnToVectorOfFeatures("service")
    val (flagEncoder, flagVecCol) = transformColumnToVectorOfFeatures("flag")

    val allColumnsWithFeatureColumnsReplacedWithVector = Set(data.columns: _*) --
      Seq("label", "protocol_type", "service", "flag") ++
      Seq(protoTypeVecCol, serviceVecCol, flagVecCol)

    val assembler = new VectorAssembler().
      setInputCols(allColumnsWithFeatureColumnsReplacedWithVector.toArray).
      setOutputCol("featureVector")

    val scaler = new StandardScaler()
      .setInputCol("featureVector")
      .setOutputCol("scaledFeatureVector")
      .setWithStd(true)
      .setWithMean(false)

    val kmeans = new KMeans().
      setSeed(Random.nextLong()).
      setK(k).
      setPredictionCol("cluster").
      setFeaturesCol("scaledFeatureVector").
      setMaxIter(40).
      setTol(1.0e-5)

    val pipeline = new Pipeline().setStages(
      Array(protoTypeEncoder, serviceEncoder, flagEncoder, assembler, scaler, kmeans))
    pipeline.fit(data)
  }

  def buildAnomalyDetector(data: DataFrame): Unit = {
    val numberOfClusters = 180
    val pipelineModel = buildMlPipeline(data, numberOfClusters)

    val kMeansModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]
    val centroids = kMeansModel.clusterCenters

    val clustered = pipelineModel.transform(data)
    val farthestDistanceBetweenTwoNormalClusters: Double
    = getDistanceOfTwoClustersThatAreMostAwayFromEachOtherFromNTopClusters(clustered, 100, centroids)

    val originalCols = data.columns
    val anomalies: DataFrame
    = findAnomalies(centroids, clustered, farthestDistanceBetweenTwoNormalClusters, originalCols)

    anomalies.show(10)
  }

  private def findAnomalies(centroids: Array[Vector],
                            clustered: DataFrame,
                            farthestDistanceBetweenTwoNormalClusters: Double,
                            originalCols: Array[String]) = {
    val anomalies = clustered.filter { row =>
      val cluster = row.getAs[Int]("cluster")
      val vec = row.getAs[Vector]("scaledFeatureVector")
      Vectors.sqdist(centroids(cluster), vec) >= farthestDistanceBetweenTwoNormalClusters
    }.select(originalCols.head, originalCols.tail: _*)
    anomalies
  }

  private def getDistanceOfTwoClustersThatAreMostAwayFromEachOtherFromNTopClusters
  (clustered: DataFrame, N: Int, centroids: Array[Vector]): Double = {
    clustered.
      select("cluster", "scaledFeatureVector").as[(Int, Vector)].
      map { case (cluster, vec) => Vectors.sqdist(centroids(cluster), vec) }.
      orderBy($"value".desc).take(N).last
  }
}
