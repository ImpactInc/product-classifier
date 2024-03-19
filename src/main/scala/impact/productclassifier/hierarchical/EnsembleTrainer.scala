package impact.productclassifier.hierarchical

import impact.productclassifier.hierarchical.EnsembleUtil._
import impact.productclassifier.taxonomy.Category
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel, LogisticRegressionSummary}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql._

import scala.collection.mutable

class EnsembleTrainer(val spark: SparkSession, val maxLevel: Int) {

  private val HASHING_TF_FEATURES = 1024
  
  private val logisticRegression: LogisticRegression = new LogisticRegression()
    .setFeaturesCol("features").setLabelCol("cat1")
    .setMaxIter(10).setTol(0.001)
//    .setElasticNetParam(0.9).setRegParam(0.0001)
  
  def trainModels(trainingSet: DataFrame, validationSet: DataFrame): CategoryModel = {
    val model = trainSubModel(trainingSet, validationSet, taxonomy.root)
    model
  }
  
  private def buildPipeline(): Pipeline = {
    new Pipeline().setStages(Array(
      tokenizer("name"),
      wordFilter("name"),
      hasher("name", HASHING_TF_FEATURES),
      assembler(Array("name"), includeManufacturer = false),
      logisticRegression))
  }

  private def trainSubModel(trainingSet: DataFrame, validationSet: DataFrame, category: Category): CategoryModel = {
    val pipeline = buildPipeline()
    val trainingSubSet = prepareData(trainingSet, category)
    val model = new CategoryModel(category.id, category.name, pipeline.fit(trainingSubSet))
    
    val logModel = model.model.stages.last.asInstanceOf[LogisticRegressionModel]
    println(s"Num features: ${logModel.numFeatures}")
    println(s"Num classes: ${logModel.numClasses}")
    println("Coefficients:")
    println(logModel.coefficientMatrix.toString())
    println(s"Num coefficients: ${logModel.coefficientMatrix.numRows * logModel.coefficientMatrix.numCols}")
    println(s"Num active: ${logModel.coefficientMatrix.numActives}")
    println(s"Num nonzero: ${logModel.coefficientMatrix.numNonzeros}")
    showTrainingResults(logModel, category)
    evaluateModel(model, validationSet, category)

    val subCategories = category.getSubCategories
      .filter(_.level <= maxLevel - 1)
      .filter(!_.isLeaf)
    if (subCategories.nonEmpty) {
      val validationSubSet = filterDataByCategory(validationSet, category)
      subCategories
        .map(subCategory => trainSubModel(trainingSubSet, validationSubSet, subCategory))
        .foreach(model.addSubModel)
    }
    model
  }
  
  private def evaluateModel(model: CategoryModel, data: DataFrame, category: Category): Unit = {
    val fullPredictions = model.model.transform(prepareData(data, category)).cache()
    val predictions = fullPredictions.select("prediction").rdd.map(_.getDouble(0))
    val labels = fullPredictions.select("cat1").rdd.map(_.getInt(0).toDouble)
    val metrics = new MulticlassMetrics(predictions.zip(labels))
    val accuracy = metrics.accuracy
    println(s"Accuracy: $accuracy")
    println("Confusion Matrix:")
    val confusion = metrics.confusionMatrix.rowIter.toIndexedSeq
    val categoryNames: Seq[String] = category.getSubCategories.map(_.name).sorted
    metrics.labels.zipWithIndex.foreach {
      case (label, index) =>
        val name = categoryNames(label.toInt)
        val row = confusion(index).toArray
          .map(_.toInt)
          .map(n => f"$n%5s")
          .mkString(" ")
        println(f"$name%-30s $row%s")
    }
    println(String.format("%-30s %-15s %s", "Category", "Precision", "Recall"))
    metrics.labels
      .map(label => (label, categoryNames(label.toInt)))
      .sortBy(_._2)
      .foreach {
        case (label, categoryName) =>
        val precision = round(metrics.precision(label), 3)
        val recall = round(metrics.recall(label), 3)
        println(f"$categoryName%-30s $precision%-15s $recall%s")
    }
  }
  
  private def showTrainingResults(logModel: LogisticRegressionModel, category: Category): Unit = {
    val trainingSummary = logModel.summary
    println("Training results for " + category.name)
    println("Objective history: " + trainingSummary.objectiveHistory.map(n => round(n, 3)).mkString(", "))
    showResults(trainingSummary, category)
  }
  
  private def showResults(summary: LogisticRegressionSummary, category: Category): Unit = {
    val accuracy = summary.accuracy
    println(s"Accuracy: $accuracy")
    
    val labels = summary.labels
    println(String.format("%-30s %-15s %s", "Category", "Precision", "Recall"))
    val recallByLabel = summary.recallByLabel
    val categoryNames: Seq[String] = category.getSubCategories.map(_.name).sorted
    summary.precisionByLabel.zipWithIndex.foreach {
      case (precision, index) =>
        val categoryName = categoryNames(labels(index).toInt)
        val precisionRounded = round(precision, 3)
        val recall = round(recallByLabel(index), 3)
        println(f"$categoryName%-30s $precisionRounded%-15s $recall%s")
    }
  }
  
  class CategoryModel(val id: Int, val name: String, val model: PipelineModel) {
    
    private val subModelsById: mutable.Map[Int, CategoryModel] = mutable.Map.empty
    private val subModelsByName: mutable.Map[String, CategoryModel] = mutable.Map.empty
    
    def addSubModel(subModel: CategoryModel): Unit = {
      subModelsById(subModel.id) = subModel
      subModelsByName(subModel.name) = subModel
    }
  }
}
