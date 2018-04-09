/**
  * Copyright (C) 2016  Databiz s.r.l.
  *
  * This program is free software: you can redistribute it and/or modify
  * it under the terms of the GNU General Public License as published by
  * the Free Software Foundation, either version 3 of the License, or
  * (at your option) any later version.
  *
  * This program is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  * GNU General Public License for more details.
  *
  * You should have received a copy of the GNU General Public License
  * along with this program.  If not, see <http://www.gnu.org/licenses/>.
  */

package it.databiz.spark.deep.learning

import it.databiz.spark.deep.learning.Conf._
import org.deeplearning4j.nn.api.OptimizationAlgorithm
import org.deeplearning4j.nn.conf.layers.{ConvolutionLayer, DenseLayer, OutputLayer, SubsamplingLayer}
import org.deeplearning4j.nn.conf.{MultiLayerConfiguration, NeuralNetConfiguration, Updater}
import org.deeplearning4j.nn.weights.WeightInit
import org.nd4j.linalg.lossfunctions.LossFunctions

/**
  * A dedicated MultiLayerConfiguration Builder to use in order to train a Convolutional Neural Network
  * from the MNIST dataset, taking advantage of Apache Spark's cluster computing.
  *
  * Created by Vincibean <andrebessi00@gmail.com> on 20/03/16.
  */
object MultiLayerConfigurationBuilder extends MultiLayerConfiguration.Builder {

  def apply(): MultiLayerConfiguration.Builder
  = new NeuralNetConfiguration.Builder()
    .seed(seed) // Random number generator seed.
    // Used for reproducibility between runs.
    .iterations(iterations) // Number of optimization iterations.
    .regularization(true) // Whether to use regularization
    // (L1, L2, dropout, etc...)
    .l2(0.0005) // L2 regularization coefficient.
    .learningRate(0.1) // Learning rate.
    .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
    // Optimization algorithm to use.
    .updater(Updater.ADAGRAD) // Gradient updater.
    .list(6) // Number of layers (not including the input layer).
    .layer(0, new ConvolutionLayer.Builder(5, 5)
    .nIn(numChannels) // Number of input neurons (channels).
    .stride(1, 1)
    .nOut(20) // Number of output neurons.
    .weightInit(WeightInit.XAVIER) // Weight initialization scheme.
    // The Xavier algorithm automatically determines the scale of
    // initialization based on the number of input and output neurons.
    .activation("relu") // Activation function:
    // rectified linear, an activation function defined as f(x) = max(0, x).
    .build())
    .layer(1, new SubsamplingLayer.Builder(SubsamplingLayer.PoolingType.MAX,
      Array(2, 2))
      .build())
    .layer(2, new ConvolutionLayer.Builder(5, 5)
      .nIn(20) // Number of input neurons (channels).
      .nOut(50) // Number of output neurons.
      .stride(2, 2)
      .weightInit(WeightInit.XAVIER) // Weight initialization scheme.
      // The Xavier algorithm automatically determines the scale of
      // initialization based on the number of input and output neurons.
      .activation("relu") // Activation function: rectified linear,
      // an activation function defined as f(x) = max(0, x).
      .build())
    .layer(3, new SubsamplingLayer.Builder(SubsamplingLayer.PoolingType.MAX,
      Array(2, 2))
      .build)
    .layer(4, new DenseLayer.Builder()
      .activation("relu") // Activation function: rectified linear,
      // an activation function defined as f(x) = max(0, x).
      .weightInit(WeightInit.XAVIER) // Weight initialization scheme.
      // The Xavier algorithm automatically determines the scale of
      // initialization based on the number of input and output neurons.
      .nOut(200) // Number of output neurons.
      .build())
    .layer(5, new OutputLayer.Builder(
      LossFunctions.LossFunction.NEGATIVELOGLIKELIHOOD)
      .nOut(outputNum) // Number of output neurons.
      .weightInit(WeightInit.XAVIER) // Weight initialization scheme.
      // The Xavier algorithm automatically determines the scale of
      // initialization based on the number of input and output neurons.
      .activation("softmax") // Activation function.
      // The softmax function, or normalized exponential,
      // is a generalization of the logistic
      // function that "squashes" a K-dimensional
      // vector of arbitrary real values to a
      // K-dimensional vector of real values in the range (0, 1)
      // that adds up to 1.
      .build())
    .backprop(true) // Whether to use backpropagation.
    .pretrain(false) // Whether to pretrain the Convolutional Neural Network.

}
