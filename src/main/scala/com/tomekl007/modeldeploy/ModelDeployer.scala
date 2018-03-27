package com.tomekl007.modeldeploy

import org.apache.spark.ml.{Pipeline, PipelineModel}

object ModelDeployer {
  //todo on production it should be some persistent storage like hdfs or other db
  private val pathToModel = "/tmp/spark-logistic-regression-model"
  private val pathToPipeline = "/tmp/unfit-lr-model"

  def saveModel(model: PipelineModel): Unit = {
    model.write.overwrite().save(pathToModel)
  }

  def savePipeline(pipeline: Pipeline): Unit = {
    pipeline.write.overwrite().save(pathToPipeline)
  }

  def loadModel(): PipelineModel = {
    PipelineModel.load(pathToModel)
  }
}
