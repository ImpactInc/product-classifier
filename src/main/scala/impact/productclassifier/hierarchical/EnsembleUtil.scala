package impact.productclassifier.hierarchical

import impact.productclassifier.feature.WordFilter
import impact.productclassifier.taxonomy.{Category, Taxonomy}
import org.apache.spark.ml.classification.{LogisticRegression, MultilayerPerceptronClassifier}
import org.apache.spark.ml.feature._
import org.apache.spark.sql.functions.{array, col, typedLit}
import org.apache.spark.sql.{Column, DataFrame}

object EnsembleUtil {

  val taxonomy: Taxonomy = new Taxonomy().populate()

  def tokenizer(featureName: String): RegexTokenizer = {
    new RegexTokenizer().setInputCol(featureName).setOutputCol(featureName + "Tokens")
      .setGaps(false).setPattern("\\w+")
  }
  
  def wordFilter(featureName: String): WordFilter = {
    new WordFilter(featureName)
  }

  def hasher(featureName: String, numFeatures: Int): HashingTF = {
    new HashingTF().setInputCol(featureName + "Tokens").setOutputCol(featureName + "Vector")
      .setNumFeatures(numFeatures)
  }

  def vectorizer(featureName: String, vocabSize: Int): CountVectorizer = {
    new CountVectorizer().setInputCol(featureName + "Tokens").setOutputCol(featureName + "Vector").setVocabSize(vocabSize)
  }
  
  def bucketizer(featureName: String, splits: Array[Double]): Bucketizer = {
    new Bucketizer().setInputCol(featureName).setOutputCol(featureName + "Vector").setSplits(splits)
  }
  
  def stringIndexer(featureName: String): StringIndexer = {
    new StringIndexer().setInputCol(featureName).setOutputCol(featureName + "Index").setStringOrderType("alphabetAsc")
  }
  
  def oneHotEncoder(featureName: String): OneHotEncoder = {
    new OneHotEncoder().setInputCol(featureName + "Index").setOutputCol(featureName + "Vector")
  }

  def assembler(featureNames: Array[String], includeManufacturer: Boolean): VectorAssembler = {
    var inputCols = featureNames.map(_ + "Hash")
    if (includeManufacturer) {
      inputCols = inputCols :+ "manufacturerVectorized" 
    }
    new VectorAssembler().setInputCols(inputCols).setOutputCol("features")
  }
  
  def logistic(feature: String, maxIter: Int, elasticNet: Double, regParam: Double): LogisticRegression = {
    new LogisticRegression().setFeaturesCol(feature).setLabelCol("cat1")
      .setMaxIter(maxIter)
      .setElasticNetParam(elasticNet).setRegParam(regParam)
  }
  
  def perceptron(feature: String, layers: Array[Int], maxIter: Int): MultilayerPerceptronClassifier = {
    // stepSize: Double
    new MultilayerPerceptronClassifier().setFeaturesCol(feature).setLabelCol("cat1")
      .setLayers(layers).setMaxIter(maxIter)
  }

  def prepareData(df: DataFrame, category: Category): DataFrame = {
    val df2 = df.withColumn("temp", array(col("manufacturer")))
      .drop(col("manufacturer")).withColumnRenamed("temp", "manufacturer")
    val filtered = filterDataByCategory(df2, category)
    normalizeCategoryIds(filtered, category)
  }

  def filterDataByCategory(trainingData: DataFrame, category: Category): DataFrame = {
    if (category.isRoot) {
      trainingData
    } else {
      val ancestors = category.getAncestors
      val categoryFilter = ancestors.zipWithIndex
        .map {
          case (id, index) => col(s"cat${index + 1}").eqNullSafe(id)
        }
        .reduce((a, b) => a.and(b))
      trainingData.filter(categoryFilter)
    }
  }

  def normalizeCategoryIds(df: DataFrame, category: Category): DataFrame = {
    val idToIndexMap: Column = typedLit(category.getSubCategoryIdIndexMap)
    val catColumnName = s"cat${category.level + 1}"
    val categoryIndex = idToIndexMap(col(catColumnName)).as(catColumnName)
    df.withColumn("temp", categoryIndex)
      .drop(catColumnName)
      .withColumnRenamed("temp", catColumnName)
  }

  def round(num: Double, places: Int): BigDecimal = {
    BigDecimal(num).setScale(places, BigDecimal.RoundingMode.HALF_UP)
  }
}
