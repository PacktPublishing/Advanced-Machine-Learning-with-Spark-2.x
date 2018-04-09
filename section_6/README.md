# Deep Learning on Spark

An example on how to train a [Convolutional neural network](https://en.wikipedia.org/wiki/Convolutional_neural_network)
on [Apache Spark](http://spark.apache.org/).   
Feel free to fork and contribute!

## Project Description
This repository aims to be an easy and well documented example on how to train a [Convolutional neural network](https://en.wikipedia.org/wiki/Convolutional_neural_network)
taking advantage of [Apache Spark](http://spark.apache.org/)'s distributed computation.   
It uses [Deeplearning4j](http://deeplearning4j.org/) on top of [Apache Spark](http://spark.apache.org/).   
The neural network is trained against digit classifications from the [MNIST](http://yann.lecun.com/exdb/mnist/) data set,
a subset of a larger set available from [NIST (National Institute of Standards and Technology)](http://www.nist.gov/).   
This example is set up to run on a local instance of Spark (i.e., it's runnable within your IDE).
To submit and run it on a cluster, remove the .setMaster() method and use Spark submit.

## Lineage
This example is a fork of [dl4j-spark-cdh5-examples](https://github.com/deeplearning4j/dl4j-spark-cdh5-examples).

## Authors
A joint effort between [ebezzi](https://github.com/Ebezzi) ([Emanuele Bezzi](https://www.linkedin.com/in/emanuelebezzi))
and [Vincibean](https://github.com/Vincibean) ([Andrea Bessi](https://www.linkedin.com/in/andre-bessi-22358083))

## License
Unless stated elsewhere, all files herein are licensed under the GPLv3 license. For more information, please see the
[LICENSE](https://github.com/Vincibean/spark-deep-learning/blob/master/LICENSE) file.
