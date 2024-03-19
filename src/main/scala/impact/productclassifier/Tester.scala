package impact.productclassifier

import impact.productclassifier.App.spark
import impact.productclassifier.EstimatorFactory.logistic
import impact.productclassifier.feature.Util.{loadWord2VecModel, oneHotEncoder, stringIndexer}
import impact.productclassifier.feature.{TokenNormalizer, TokenizerFactory}
import impact.productclassifier.taxonomy.{Category, Taxonomy}
import ml.dmlc.xgboost4j.scala.spark.XGBoostClassificationModel
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, LogisticRegressionModel, MultilayerPerceptronClassificationModel, RandomForestClassificationModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.functions.vector_to_array
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Column, DataFrame, SaveMode}
import org.apache.spark.storage.StorageLevel

import java.time.Instant
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

object Tester {

  import spark.implicits._

  private val taxonomy: Taxonomy = new Taxonomy().populate()
  
  private val categoryDepth: Int = 7
  private val numPerCategory: Int = 10000
  private val trainFrac: Double = 0.75
  private val minPerCategory = 1000

  def run(): Unit = {
    println(s"Category Depth: $categoryDepth")
    println(s"Sample size per category: $numPerCategory")
    println(s"Train fraction: $trainFrac")
    val features = Array(
      "gender",
      "ageGroup",
      "name",
      "description",
    ).map(col)
    val catCols = (1 to categoryDepth).map(i => col(s"cat$i"))
    val df = DataLoader.readAllData(features ++ catCols: _*)
    val normalized = normalizeCategoryIds(df, categoryDepth)

    val (trainSet, testSet) = DataLoader.sampleAndSplitDataSet(normalized, numPerCategory, categoryDepth, trainFrac)
    trainSet.persist(StorageLevel.DISK_ONLY)
    
    val name2Vec: Word2Vec = new Word2Vec()
      .setInputCol("nameTokens").setOutputCol("nameVector")
      .setVectorSize(300)
      .setWindowSize(5)
      .setMinCount(100)
      .setMaxIter(10)
      .setNumPartitions(128)
    val name2VecModel = loadWord2VecModel(name2Vec, "gs://product-classifier/model/name2vec")
    
    val desc2Vec: Word2Vec = new Word2Vec()
      .setInputCol("descriptionTokens").setOutputCol("descriptionVector")
      .setVectorSize(300)
      .setWindowSize(5)
      .setMinCount(700)
      .setMaxIter(1)
      .setNumPartitions(128)
    val desc2VecModel = loadWord2VecModel(desc2Vec, "gs://product-classifier/model/desc2vec")
    
    val pipeline = new Pipeline().setStages(Array(
      stringIndexer("gender"),
      stringIndexer("ageGroup"),
      oneHotEncoder("gender"),
      oneHotEncoder("ageGroup"),
      TokenizerFactory.tokenizer("name"),
      TokenizerFactory.tokenizer("description"),
      new TokenNormalizer("name"),
      new TokenNormalizer("description"),
      name2VecModel,
      desc2VecModel,
      new VectorAssembler().setOutputCol("features").setInputCols(Array(
        "genderVector",
        "ageGroupVector",
        "nameVector",
        "descriptionVector",
      )),
      logistic("target", 125, 0.9, 0.0).setMaxBlockSizeInMB(256)
    ))

    printPipelineParams(pipeline)
    evaluateOnce(pipeline, "target", trainSet, testSet)
  }
  
  private def evaluate(pipeline: Pipeline, targetCol: String, dataset: DataFrame): Unit = {
    val numSplits = 3
    val splits: Seq[DataFrame] = splitDataset(dataset, numSplits)
    splits.foreach(_.persist(StorageLevel.DISK_ONLY))
    
    val trainAccs = ArrayBuffer[Double]()
    val testAccs = ArrayBuffer[Double]()
    for (i <- 0 until  numSplits) {
      val trainSet = splits.zipWithIndex.filter(_._2 != i).map(_._1).reduce((a, b) => a.union(b))
      val testSet = splits(i)
      val (trainMetrics, testMetrics) = evaluateOnce(pipeline, targetCol, trainSet, testSet)
      trainAccs.append(trainMetrics.accuracy)
      testAccs.append(testMetrics.accuracy)
    }
    
    val trainAcc = roundNum(trainAccs.sum / numSplits)
    val testAcc = roundNum(testAccs.sum / numSplits)
    println(f"Average accuracy: $trainAcc, $testAcc")
    val trainResults = trainAccs.map(x => roundNum(x)).map(x => f"$x%-8s").mkString
    val testResults = testAccs.map(x => roundNum(x)).map(x => f"$x%-8s").mkString
    println(s"Train results: $trainResults")
    println(s"Test results: $testResults")

    splits.foreach(_.unpersist())
  }

  private def evaluateOnce(pipeline: Pipeline, targetCol: String, dataset: DataFrame): Unit = {
    val (trainSet, testSet) = DataLoader.sampleAndSplitDataSet(dataset, 50000, 2, 0.75)
    evaluateOnce(pipeline, targetCol, trainSet.repartition(512), testSet.repartition(512))
  }
  
  private def evaluateOnce(
                            pipeline: Pipeline, 
                            targetCol: String, 
                            trainSet: DataFrame, 
                            testSet: DataFrame): (MulticlassMetrics, MulticlassMetrics) = {
    println(s"${Instant.now()} Train partitions: ${trainSet.rdd.getNumPartitions}")
    println(s"${Instant.now()} Test partitions: ${testSet.rdd.getNumPartitions}")
    
    val evaluator = new MulticlassClassificationEvaluator().setLabelCol(targetCol)
    val featurePipeline = new Pipeline().setStages(pipeline.getStages.slice(0, pipeline.getStages.length - 1))
    if (trainSet.storageLevel.equals(StorageLevel.NONE)) {
      trainSet.persist(StorageLevel.DISK_ONLY)
    }
    println(trainSet.count())
    println(Instant.now())
    val featureModel = featurePipeline.fit(trainSet)
    val features = featureModel.transform(trainSet)
      .select($"features", $"target")
      .repartition(256)
      .persist(StorageLevel.DISK_ONLY)
    println(Instant.now())
    println(s"${Instant.now()} Features partitions: ${features.rdd.getNumPartitions}")
    val predictorPipeline = new Pipeline().setStages(Array(pipeline.getStages.last))
    val predictorModel = predictorPipeline.fit(features)
    println(s"${Instant.now()} Finished fitting predictor")
    val model = new Pipeline().setStages(featureModel.stages ++ predictorModel.stages).fit(trainSet)

    val trainResults = predictorModel.transform(features).persist(StorageLevel.DISK_ONLY)
    features.unpersist()
    trainSet.unpersist()
    testSet.persist(StorageLevel.DISK_ONLY)
    val testResults = model.transform(testSet).persist(StorageLevel.DISK_ONLY)
    val trainMetrics = evaluator.getMetrics(trainResults)
    val testMetrics = evaluator.getMetrics(testResults)
    println(f"${roundNum(trainMetrics.accuracy)}%-8s | ${roundNum(testMetrics.accuracy)}%8s")
    println(s"${Instant.now()} Persisting pipeline model")
    model.save(s"gs://product-classifier/model/${Instant.now()}")
    model.stages.last match {
      case logModel: LogisticRegressionModel =>
        val numCoefs = logModel.coefficientMatrix.numRows * logModel.coefficientMatrix.numCols
        println(logModel.coefficientMatrix.numNonzeros + " / " + numCoefs)
        println(logModel.summary.objectiveHistory.map(x => roundNum(x)).mkString(", "))
      case neuralNetModel: MultilayerPerceptronClassificationModel =>
        println(neuralNetModel.weights.numNonzeros + " / " + neuralNetModel.weights.size)
        println(neuralNetModel.summary.objectiveHistory.map(x => roundNum(x)).mkString(", "))
      case decisionTreeModel: DecisionTreeClassificationModel =>
        println(s"Tree depth: ${decisionTreeModel.depth}")
        println(s"Num nodes: ${decisionTreeModel.numNodes}")
        println("Feature importance:")
        println(decisionTreeModel.featureImportances.toArray.map(roundNum).mkString(", "))
      case randomForestModel: RandomForestClassificationModel =>
        println(s"Num nodes: ${randomForestModel.totalNumNodes}")
        println("Feature importance:")
        println(randomForestModel.featureImportances.toArray.map(roundNum).mkString(", "))
      case xgbModel: XGBoostClassificationModel =>
        println(xgbModel.summary.trainObjectiveHistory.map(x => roundNum(x)).mkString(", "))
      case _ =>
    }
    model.stages.filter(_.isInstanceOf[Word2VecModel]).map(_.asInstanceOf[Word2VecModel]).foreach(word2VecModel => {
      println(s"Vocab size: ${word2VecModel.getVectors.select(lit(1)).count()}")
    })
    val trainNames = trainSet.select($"category").distinct().collect().map(_.getAs[String]("category")).sorted
    println(s"Num targets: ${trainNames.length}")
    val names = testSet.select($"category").distinct().collect().map(_.getAs[String]("category")).sorted
    println(s"Num targets: ${names.length}")
    val nameToIndex = getNameToIndexMap.keys.toArray.sorted
    println("Test confusion:")
    testSet.unpersist()
    printConfusionMatrix(nameToIndex, testMetrics.confusionMatrix, testResults)
    
    val confidenceThresholds = Array(0.0, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.98, 0.99)
    val maxProb = array_max(vector_to_array($"probability")).as("maxProbability")
    val train = trainResults.select($"target", $"prediction", maxProb).repartition(512).persist(StorageLevel.DISK_ONLY)
    val test = testResults.select($"target", $"prediction", maxProb).repartition(512).persist(StorageLevel.DISK_ONLY)
    val trainTotal = train.count()
    val testTotal = test.count()
    val nameMap = typedLit(nameToIndex)
    val splitTarget = split(nameMap($"target".cast(IntegerType)), " > ")
    val splitTargetCols = (0 until categoryDepth).map(i => (s"target${i + 1}", splitTarget(i))).toMap
    val splitPredict = split(nameMap($"prediction".cast(IntegerType)), " > ")
    val splitPredictCols = (0 until categoryDepth).map(i => (s"predict${i + 1}", splitPredict(i))).toMap
    var splitDf = test
    for ((name, column) <- splitTargetCols ++ splitPredictCols) {
      splitDf = splitDf.withColumn(name, column)
    }
    def computeLevelAcc(level: Int): Double = {
      var correct = splitDf
      for (i <- 1 to level) {
        val target = col(s"target$i")
        val predicted = col(s"predict$i")
        val bothNull = target.isNull.and(predicted.isNull)
        val bothSame = target.equalTo(predicted)
        correct = correct.filter(bothNull || bothSame)
      }
      val numCorrect = correct.count()
      (100.0 * numCorrect) / testTotal
    }
    for (i <- 1 to categoryDepth) {
      println(s"Level $i accuracy: ${computeLevelAcc(i)}")
    }
    println(f"${"Threshold"}%15s | ${"Train Acc %"}%15s | ${"Train Coverage %"}%15s | ${"Test Acc %"}%15s | ${"Test Coverage %"}%15s")
    val isCorrect = $"target".equalTo($"prediction")
    for (threshold <- confidenceThresholds) {
      val isAboveThreshold = $"maxProbability".geq(threshold)
      val trainCovered = train.filter(isAboveThreshold).count()
      val testCovered = test.filter(isAboveThreshold).count()
      val trainAccuracy = (100.0 * train.filter(isCorrect.and(isAboveThreshold)).count()) / (trainCovered + 1)
      val testAccuracy = (100.0 * test.filter(isCorrect.and(isAboveThreshold)).count()) / (testCovered + 1)
      val trainCoverage = (100.0 * trainCovered) / trainTotal
      val testCoverage = (100.0 * testCovered) / testTotal
      println(f"$threshold%15s " +
        f"| ${roundNum(trainAccuracy, 3)}%15s | ${roundNum(trainCoverage, 3)}%15s " +
        f"| ${roundNum(testAccuracy, 3)}%15s | ${roundNum(testCoverage, 3)}%15s")
    }
    
    println("=========================================================================================================")
    testResults
      .limit(100000)
      .select($"name", $"category", typedLit(nameToIndex).apply($"prediction".cast(IntegerType)).as("prediction"))
      .sort("name")
      .show(100, truncate = false)
    println("=========================================================================================================")
    
    trainResults.unpersist()
    testResults.unpersist()
    train.unpersist()
    test.unpersist()
    
    val cols = testResults.columns.intersect(Array("name", "description", "manufacturer", "target", "probability", "rawPrediction", "prediction"))
    val rawProbability = transform(vector_to_array($"rawPrediction"), x => round(pow(typedLit(1.0) + exp(-x), -1.0), 3))
    testResults
      .select(cols.map(col): _*)
      .withColumn("softmax", transform(vector_to_array($"probability"), x => round(x, 3)))
      .withColumn("rawProbability", rawProbability)
      .drop("rawPrediction", "probability")
      .filter($"target".notEqual($"prediction"))
      .show(25, truncate = false)
    
    val homeDepot = spark.read
      .option("delimiter", ";").option("header", "true")
      .csv("gs://product-classifier/top-home-depot-products.csv")
      .select(
        $"id",
        $"name",
        $"description",
        lit("null").as("gender"),
        lit("null").as("ageGroup"),
        $"ai_gpc_opt", 
        $"category".as("advCategory")
      ).as("hd")
    var homeDepotCurated = spark.read
      .option("delimiter", ";").option("header", "true")
      .csv("gs://product-classifier/top-home-depot-products-curated.csv")
      .select(
        $"id",
        $"ai_title".as("name"), 
        lit("").as("description"), 
        lit("null").as("gender"), 
        lit("null").as("ageGroup"),
        $"ai_gpc_opt", $"correct", $"category".as("advCategory")
      ).as("hdc")
    homeDepotCurated = homeDepot.join(homeDepotCurated, "id")
      .select($"hd.name".as("name"), $"hd.description".as("description"), $"hd.gender".as("gender"), 
        $"hd.ageGroup".as("ageGroup"), $"hd.advCategory".as("advCategory"), 
        $"hdc.ai_gpc_opt".as("ai_gpc_opt"), $"hdc.correct".as("correct"))
    
    val homeDepotResults = model.transform(homeDepotCurated)
      .select($"name", $"advCategory", $"ai_gpc_opt", $"correct", 
        typedLit(nameToIndex).apply($"prediction".cast(IntegerType)).as("prediction"))
    homeDepotResults.show(100, truncate = false)
    homeDepotResults.write.mode(SaveMode.Overwrite)
      .option("delimiter", ";").option("header", "true")
      .csv("gs://product-classifier/home-depot-results")
    
    (trainMetrics, testMetrics)
  }
  
  private def sampleDataset(df: DataFrame, numPerCategory: Int): DataFrame = {
    DataLoader.sampleDataSet(df.withColumn("rng", rand()), numPerCategory, 1)
  }

  private def sampleDataset(df: DataFrame, numPerCategory: Int, categoryDepth: Int): DataFrame = {
    DataLoader.sampleDataSet(df.withColumn("rng", rand()), numPerCategory, categoryDepth)
  }

  private def splitDataset(df: DataFrame, numSplits: Int): Seq[DataFrame] = {
    val sizes: Map[Int, Long] = df
      .groupBy($"target").count()
      .collect()
      .map(row => (row.getAs[Int]("target"), row.getAs[Long]("count")))
      .toMap
    val trainingSizeByCategory = round(typedLit(sizes).apply($"target"))

    val w = Window.partitionBy($"target").orderBy($"rng")
    val rows = df.withColumn("rn", row_number.over(w))
    val splitSize: Column = trainingSizeByCategory / numSplits
    val splits = (0 until numSplits)
      .map(i => rows.where($"rn".between(splitSize * i, splitSize * (i + 1) - 1)))
      .map(_.drop("rn"))
    splits
  }

  private def normalizeCategoryIds(df: DataFrame, category: Category): DataFrame = {
    val nameToIndexMap: Column = typedLit(category.getSubCategoryNameIndexMap)
    val catColumnName = s"cat${category.level + 1}"
    val categoryIndex = nameToIndexMap(col(catColumnName)).as(catColumnName)
    df.withColumn("target", categoryIndex)
  }

  private def normalizeCategoryIds(df: DataFrame, maxDepth: Int): DataFrame = {
    val sizes: Map[String, Int] = DataLoader.getLabelFrequencyMap(maxDepth, Int.MaxValue)
    def rollUp(entry: (String, Int)): (String, String) = {
      if (entry._2 < minPerCategory) {
        val path = entry._1.split(" > ")
        if (path.length > 1) {
          @tailrec
          def findParent(level: Int): String = {
            val name = path.slice(0, level).mkString(" > ")
            if (level == 1 || (sizes(name) >= minPerCategory)) {
              name
            } else {
              findParent(level - 1)
            }
          }
          (entry._1, findParent(path.length - 1))
        } else {
          (entry._1, entry._1)
        }
      } else {
        (entry._1, entry._1)
      }
    }
//    val rolledUp: Map[String, String] = sizes.map(entry => rollUp(entry))
    val catCols = (1 to maxDepth).map(i => s"cat$i")
    val catColumn = regexp_replace(concat_ws(" > ", catCols.map(col): _*), "(?: > )* > \\Z", "")
    val df2 = df
      .na.fill("", catCols)
      .withColumn("category", catColumn)
//      .withColumn("category", typedLit(rolledUp).apply(catColumn))
    
    val sizesCol = typedLit(sizes)
    val hasEnoughExamples = (catCol: Column) => sizesCol(catCol).geq(minPerCategory)
    val nameToIndexMap: Map[String, Int] = getNameToIndexMap
    //    println(rolledUp.toArray.sortBy(_._1).map(entry => s"${entry._1} ==> ${entry._2}").mkString("\n"))
    //    println(nameToIndexMap.toArray.sortBy(_._2).map(entry => s"${entry._2}: ${entry._1}").mkString("\n"))
    
//    val df3 = df2.select($"category").limit(10000000).cache()
//    println("===============================================================================")
//    df3.distinct().orderBy($"category").show(200, truncate = false)
//    println(df3.filter(hasEnoughExamples($"category")).count())
//    val df4 = df3
//      .filter(hasEnoughExamples($"category"))
//      .withColumn("target", typedLit(nameToIndexMap).apply($"category"))
//    println(df4.filter($"target".isNotNull).count())
//    df4.select($"category", $"target").distinct().show(200, truncate = false)
//    df3.unpersist()
    
    // Necessary for DecisionTreeClassifier when numLabels > 100
    val metadata = NominalAttribute.defaultAttr.withName("target").withNumValues(nameToIndexMap.size).toMetadata()
    df2
      .filter(hasEnoughExamples($"category"))
      .withColumn("target", typedLit(nameToIndexMap).apply($"category").as("target", metadata))
      .filter($"target".isNotNull)
  }
  
  private def getNameToIndexMap: Map[String, Int] = {
    val sizes: Map[String, Int] = DataLoader.getLabelFrequencyMap(categoryDepth, Int.MaxValue)
    var cats = taxonomy.root.getSubCategories
    val catNames: ArrayBuffer[String] = ArrayBuffer()
    for (_ <- 0 until categoryDepth) {
      catNames.appendAll(cats.map(_.getPath))
      cats = cats.flatMap(_.getSubCategories)
    }
    val nameToIndexMap: Map[String, Int] = catNames
      .filter(cat => sizes.contains(cat) && sizes(cat) >= minPerCategory)
      .sorted.zipWithIndex.toMap
    println(s"==================================================\nNum targets: ${nameToIndexMap.size}")
    nameToIndexMap
  }

  private def roundNum(num: Double): BigDecimal = {
    roundNum(num, 3)
  }

  private def roundNum(num: Double, places: Int): BigDecimal = {
    BigDecimal(num).setScale(places, BigDecimal.RoundingMode.HALF_UP)
  }
  
  private def printConfusionMatrix(labels: Seq[String], matrix: Matrix, testResults: DataFrame): Unit = {
    val values = matrix.rowIter.toArray.map(_.toArray.map(_.toInt))
    val stats: ArrayBuffer[(String, String, Int)] = ArrayBuffer()
    for (row <- values.indices) {
      for (column <- values(0).indices) {
        stats.append((labels(row), labels(column), values(row)(column)))
      }
    }
    val sizes: Map[String, Long] = testResults
      .groupBy($"category").count().collect()
      .map(elem => (elem.getAs[String]("category"), elem.getAs[Long]("count"))).toMap
    
    // Most confused categories by count
    for ((actual, predicted, count) <- stats.filter(elem => elem._1 != elem._2).sortBy(-_._3).take(50)) {
      if (count >= 100) {
        println(f"$actual%-125s  - $predicted%125s  | $count%6s | ${sizes(actual)}%9s | ${sizes(predicted)}%9s")
      }
    }

    println("=========================================================================================================")
    println("=========================================================================================================")
    // Most confused categories by fraction misclassified
    stats
      .filter(elem => elem._1 != elem._2)
      .map(elem => (elem._1, elem._2, elem._3.toDouble / sizes(elem._1)))
      .sortBy(-_._3).take(50)
      .foreach {case (actual, predicted, fraction) => {
        println(f"$actual%-125s  - $predicted%125s  | ${roundNum(100 * fraction)}%6s | ${sizes(actual)}%9s | ${sizes(predicted)}%9s")
      }}
  }

  private def printPipelineParams(pipeline: Pipeline): Unit = {
    println("Pipeline info:")
    for (stage <- pipeline.getStages) {
      val params = stage.extractParamMap().toSeq
        .map(pair => (pair.param.parent + "_" + pair.param.name, pair.value))
        .filter(!_._1.contains("vecAssembler"))
        .filter(pair => !(pair._1.contains("Col") && !pair._1.contains("inputCol")))
        .sortBy(_._1)
        .map(pair => f"\t${pair._1}%-30s: ${pair._2}%15s")
        .mkString("\n")
      // TODO: handle array params
      println(params)
    }
  }

  private def printDFSize(df: DataFrame): Unit = {
    df.persist(StorageLevel.MEMORY_AND_DISK_SER).foreach(_ => ())
    val catalystPlan = df.queryExecution.logical
    val sizeInKB: BigInt = spark.sessionState.executePlan(catalystPlan).optimizedPlan.stats.sizeInBytes / 1000
    val sizeInMB = sizeInKB.toDouble / 1000
    println(f"DataFrame size (MB): $sizeInMB%.3f")
    df.unpersist()
  }
}
