package impact.productclassifier

import ml.dmlc.xgboost4j.scala.spark.XGBoostClassifier
import org.apache.spark.ml.classification.{DecisionTreeClassifier, LogisticRegression, MultilayerPerceptronClassifier, NaiveBayes, RandomForestClassifier}

object EstimatorFactory {

  def logistic(targetCol: String, maxIter: Int, elasticNet: Double, regParam: Double): LogisticRegression = {
    new LogisticRegression()
      .setFeaturesCol("features").setLabelCol(targetCol)
      .setMaxIter(maxIter)
      .setElasticNetParam(elasticNet)
      .setRegParam(regParam)
  }
  
  def neuralNetwork(targetCol: String, maxIter: Int, stepSize: Double, layers: Array[Int]): MultilayerPerceptronClassifier = {
    new MultilayerPerceptronClassifier()
      .setFeaturesCol("features").setLabelCol(targetCol)
      .setMaxIter(maxIter)
      .setSolver("gd")
      .setStepSize(stepSize)
      .setLayers(layers)
  }
  
  def naiveBayes(targetCol: String, modelType: String): NaiveBayes = {
    new NaiveBayes()
      .setFeaturesCol("features").setLabelCol(targetCol)
      .setModelType(modelType)
  }

  def decisionTree(targetCol: String, maxDepth: Int, maxBins: Int): DecisionTreeClassifier = {
    new DecisionTreeClassifier()
      .setFeaturesCol("features").setLabelCol(targetCol)
      .setMaxDepth(maxDepth).setMaxBins(maxBins)
  }
  
  def randomForest(targetCol: String, maxDepth: Int, maxBins: Int, numTrees: Int): RandomForestClassifier = {
    new RandomForestClassifier()
      .setFeaturesCol("features").setLabelCol(targetCol)
      .setMaxDepth(maxDepth).setMaxBins(maxBins).setNumTrees(numTrees)
  }
  
  def xgbClassifier(targetCol: String): XGBoostClassifier = {
    new XGBoostClassifier()
      .setFeaturesCol("features").setLabelCol(targetCol)
      .setTreeMethod("hist").setSinglePrecisionHistogram(true)
      .setObjective("multi:softmax")
      .setMaxBins(32).setMaxDepth(6)
      .setNumRound(10)
      .setNumWorkers(16)
  }
  
}
