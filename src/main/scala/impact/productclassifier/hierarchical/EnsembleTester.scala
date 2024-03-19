package impact.productclassifier.hierarchical

import impact.productclassifier.DataLoader
import org.apache.spark.ml.classification.{LogisticRegressionModel, MultilayerPerceptronClassificationModel}
import org.apache.spark.ml.feature.CountVectorizerModel
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{DataFrame, SparkSession}
import EnsembleUtil._

import scala.collection.mutable.ArrayBuffer

class EnsembleTester(val spark: SparkSession, val maxLevel: Int) {

  def test(): Unit = {
    println(taxonomy.root.getSubCategories.size)
    val featureNames = Array(
      "name",
//      "description",
//      "labels",
    )
    val includeManufacturer = false
    println(featureNames.mkString("Features: ", ", ", ""))
    println(s"includeManufacturer = $includeManufacturer")

    val hyperParams = Array(1024)//, 1024, 2048, 4096, 8192, 16384)
    val n = 5

    val transformers = featureNames.flatMap(feature =>
      Array(
        tokenizer(feature),
        wordFilter(feature)
      ))
    val hasWordFilter = (transformers.length / featureNames.length) > 1
    println("hasWordFilter = " + hasWordFilter)
    
    val hiddenLayerSize = 100
    println("hiddenLayerSize = " + hiddenLayerSize)

    def prep(df: DataFrame): DataFrame = {
      var result = df
      for (transform <- transformers) {
        result = transform.transform(result)
      }
      result
    }

    val maxIter = 40

    def test(trainSet: DataFrame, testSet: DataFrame, nFeatures: Int): (Double, Double) = {
      var stages: Array[PipelineStage] = featureNames.map(feature => hasher(feature, nFeatures))
      if (includeManufacturer) {
//        stages = stages :+ vectorizer("manufacturer")
      }
      val inputLayerSize = featureNames.length * (nFeatures + (if (includeManufacturer) 1 else 0))
      println(inputLayerSize)
      val neuralNet = perceptron("features", Array(inputLayerSize, hiddenLayerSize, 19), maxIter)
      println(neuralNet.getMaxIter + " -------------- " + neuralNet.getStepSize)
      stages = stages :+ assembler(featureNames, includeManufacturer) :+ neuralNet
      val pipeline = new Pipeline().setStages(stages)
      val model = pipeline.fit(trainSet)
      if (includeManufacturer) {
        val vectorModel = model.stages(model.stages.length - 3).asInstanceOf[CountVectorizerModel]
        println("Vocabulary size: " + vectorModel.vocabulary.length)
      }

      evaluateModelAccuracy(model.stages.last.asInstanceOf[MultilayerPerceptronClassificationModel], model.transform(testSet))
    }

    val trainAccs: Map[Int, ArrayBuffer[Double]] = hyperParams.map(n => (n, ArrayBuffer[Double]())).toMap
    val testAccs: Map[Int, ArrayBuffer[Double]] = hyperParams.map(n => (n, ArrayBuffer[Double]())).toMap

    for (i <- 0 until n) {
      println("Iteration " + (i + 1))
      val data = DataLoader.prepareTrainingData(DataLoader.readAllTrainingData(), 1)
      val (trainingSet, validationSet) = DataLoader.sampleDataSet(data, 40000, 0.75)
      val trainSet = prepareData(prep(trainingSet), taxonomy.root)
        .drop("name", "description", "labels")//.cache()
      val testSet = prepareData(prep(validationSet), taxonomy.root)
        .drop("name", "description", "labels")//.cache()

      hyperParams.foreach(numFeatures => {
        val (trainAcc, testAcc) = test(trainSet, testSet, numFeatures)
        trainAccs(numFeatures) += trainAcc
        testAccs(numFeatures) += testAcc
      })

//      trainSet.unpersist(true)
//      testSet.unpersist(true)
    }
    hyperParams.foreach(numFeatures => {
      val trainScores = trainAccs(numFeatures)
      val testScores = testAccs(numFeatures)
      val trainAcc = round(trainScores.sum / n, 3)
      val testAcc = round(testScores.sum / n, 3)
      println(f"Average results for $numFeatures: $trainAcc, $testAcc")
      val trainResults = trainScores.map(x => round(x, 2)).map(x => f"$x%-5s").mkString
      val testResults = testScores.map(x => round(x, 2)).map(x => f"$x%-5s").mkString
      println(s"Train results: $trainResults")
      println(s"Test results: $testResults")
      println("-" * 100)
    })
  }

  def testHyperParameters(): Unit = {
    val featureNames = Array(
//      "name",
      "description",
//      "labels",
    )
    val includeManufacturer = false
    println(featureNames.mkString("Features: ", ", ", ""))
    println(s"includeManufacturer = $includeManufacturer")
    
    val hyperParams = Array(512, 1024, 2048, 4096, 8192, 16384)
    val n = 5

    val transformers = featureNames.flatMap(feature =>
      Array(
        tokenizer(feature),
//        wordFilter(feature)
      ))
    val hasWordFilter = (transformers.length / featureNames.length) > 1
    println("hasWordFilter = " + hasWordFilter)

    def prep(df: DataFrame): DataFrame = {
      var result = df
      for (transform <- transformers) {
        result = transform.transform(result)
      }
      result
    }

    val maxIter = 7
//        val (elasticNet, regParam) = (0.9, 0.0001)
//    val (elasticNet, regParam) = (0.9, 0.001)
        val (elasticNet, regParam) = (0.0, 0.0)
    println(s"MaxIter = $maxIter\nElastic Net = $elasticNet\nRegParam = $regParam")

    def test(trainSet: DataFrame, testSet: DataFrame, nFeatures: Int): (Double, Double) = {
      var stages: Array[PipelineStage] = featureNames.map(feature => hasher(feature, nFeatures))
      if (includeManufacturer) {
//        stages = stages :+ vectorizer("manufacturer")
      }
      stages = stages :+ assembler(featureNames, includeManufacturer) :+ logistic("features", maxIter, elasticNet, regParam)
      val pipeline = new Pipeline().setStages(stages)
      val model = pipeline.fit(trainSet)
      if (includeManufacturer) {
        val vectorModel = model.stages(model.stages.length - 3).asInstanceOf[CountVectorizerModel]
        println("Vocabulary size: " + vectorModel.vocabulary.length)
      }

      evaluateModelAccuracy(model.stages.last.asInstanceOf[LogisticRegressionModel], model.transform(testSet))
    }

    val trainAccs: Map[Int, ArrayBuffer[Double]] = hyperParams.map(n => (n, ArrayBuffer[Double]())).toMap
    val testAccs: Map[Int, ArrayBuffer[Double]] = hyperParams.map(n => (n, ArrayBuffer[Double]())).toMap

    for (i <- 0 until n) {
      println("Iteration " + (i + 1))
      val data = DataLoader.prepareTrainingData(DataLoader.readAllTrainingData(), 1)
      val (trainingSet, validationSet) = DataLoader.sampleDataSet(data, 4000, 0.75)
      val trainSet = prepareData(prep(trainingSet), taxonomy.root)
        .drop("name", "description", "labels").cache()
      val testSet = prepareData(prep(validationSet), taxonomy.root)
        .drop("name", "description", "labels").cache()

      hyperParams.foreach(numFeatures => {
        val (trainAcc, testAcc) = test(trainSet, testSet, numFeatures)
        trainAccs(numFeatures) += trainAcc
        testAccs(numFeatures) += testAcc
      })

      trainSet.unpersist(true)
      testSet.unpersist(true)
    }
    hyperParams.foreach(numFeatures => {
      val trainScores = trainAccs(numFeatures)
      val testScores = testAccs(numFeatures)
      val trainAcc = round(trainScores.sum / n, 3)
      val testAcc = round(testScores.sum / n, 3)
      println(f"Average results for $numFeatures: $trainAcc, $testAcc")
      val trainResults = trainScores.map(x => round(x, 2)).map(x => f"$x%-5s").mkString
      val testResults = testScores.map(x => round(x, 2)).map(x => f"$x%-5s").mkString
      println(s"Train results: $trainResults")
      println(s"Test results: $testResults")
      println("-" * 100)
    })
  }
  
  private def evaluateModelAccuracy(logModel: LogisticRegressionModel, fullPredictions: DataFrame): (Double, Double) = {
//    val logModel = model.stages.last.asInstanceOf[LogisticRegressionModel]
//    val logModel = model.stages.last.asInstanceOf[MultilayerPerceptronClassificationModel]
//    val fullPredictions = model.transform(testSet)
    fullPredictions.cache()
    val predictions = fullPredictions.select("prediction").rdd.map(_.getDouble(0))
    val labels = fullPredictions.select("cat1").rdd.map(_.getInt(0).toDouble)
    fullPredictions.unpersist(true)
    val metrics = new MulticlassMetrics(predictions.zip(labels))
    println(logModel.summary.objectiveHistory.map(x => round(x, 4)).mkString("", ", ", ""))
    println(s"${round(logModel.summary.accuracy, 3)}, ${round(metrics.accuracy, 3)}")
    val numCoefs = logModel.coefficientMatrix.numRows * logModel.coefficientMatrix.numCols
    println(logModel.coefficientMatrix.numNonzeros + " / " + numCoefs)
    (logModel.summary.accuracy, metrics.accuracy)
  }

  private def evaluateModelAccuracy(model: MultilayerPerceptronClassificationModel, fullPredictions: DataFrame): (Double, Double) = {
    fullPredictions.cache()
    val predictions = fullPredictions.select("prediction").rdd.map(_.getDouble(0))
    val labels = fullPredictions.select("cat1").rdd.map(_.getInt(0).toDouble)
    fullPredictions.unpersist(true)
    val metrics = new MulticlassMetrics(predictions.zip(labels))
    println(model.summary.objectiveHistory.map(x => round(x, 4)).mkString("", ", ", ""))
    println(s"${round(model.summary.accuracy, 3)}, ${round(metrics.accuracy, 3)}")
    println(model.numFeatures)
    println(model.weights.numNonzeros + " / " + model.weights.size)
    (model.summary.accuracy, metrics.accuracy)
  }
}
