package impact.productclassifier.analysis

import impact.productclassifier.DataLoader
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{LDA, LocalLDAModel}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import impact.productclassifier.App.spark
import impact.productclassifier.feature.{StopWords, TokenNormalizer, TokenizerFactory}

object TopicAnalyzer {

  import spark.implicits._

  private val tokenizer: RegexTokenizer = TokenizerFactory.tokenizer("")
    .setInputCol("text").setOutputCol("textTokens")
  
  private val vectorizer: CountVectorizer = new CountVectorizer()
    .setInputCol("filteredTokens").setOutputCol("vectors")
    .setVocabSize(20000)
  
  private val categories: Array[String] = Array(
    "Apparel & Accessories > Clothing",
    "Toys & Games > Toys",
    "Media > Books"
  )
  
  def analyze(): Unit = {
//    val data = DataLoader.readAggregatedData($"name", $"cat1")
//      .select($"name".as("text"), $"cat1")
    val data = DataLoader.readAggregatedData($"description".as("text"), $"cat1")
//      .select($"description", concat_ws(" ", $"cat1", $"description").as("text"), $"cat1")
//      .withColumn("category", concat_ws(" > ", $"cat1"))
//      .filter($"category".isInCollection(categories.map(_.toLowerCase)))
    val df = DataLoader.sampleDataSet(data.withColumn("rng", rand()), 20000, 1).cache()
    
    val pipeline1 = new Pipeline().setStages(Array(
      tokenizer,
      new TokenNormalizer("text"),
      new StopWordsRemover().setInputCol("textTokens").setOutputCol("filteredTokens")
        .setCaseSensitive(true).setStopWords(StopWords.LDA),
    ))

    val pipeline2 = new Pipeline().setStages(Array(
      vectorizer,
      new LDA().setFeaturesCol("vectors")
        .setK(19)
        .setSubsamplingRate(0.01).setMaxIter(100)
        .setOptimizeDocConcentration(true)
        .setLearningOffset(64)
    ))
    
//    val pipeline = new Pipeline().setStages(Array(
//      tokenizer,
//      new TokenNormalizer("text"),
//      new StopWordsRemover().setInputCol("textTokens").setOutputCol("filteredTokens")
//        .setCaseSensitive(true).setStopWords(StopWords.LDA),
//      vectorizer,
//      new LDA().setFeaturesCol("vectors")
//        .setK(19)
//        .setSubsamplingRate(0.01).setMaxIter(100)
//        .setOptimizeDocConcentration(true)
//        .setLearningOffset(64)
//    ))
    
    val prepped = pipeline1.fit(df).transform(df)
      .withColumn("temp", array_union(array($"cat1"), $"filteredTokens"))
      .drop($"filteredTokens")
      .withColumnRenamed("temp", "filteredTokens")
    
    val model = pipeline2.fit(prepped)
    
    val vocabulary: Array[String] = model.stages(model.stages.length - 2).asInstanceOf[CountVectorizerModel].vocabulary
    val indexToWordTransform = udf((arr: Seq[Int]) => arr.map(v => vocabulary(v)))
    
    val topicModel = model.stages.last.asInstanceOf[LocalLDAModel]
    val topics = topicModel.describeTopics(25)
    
    println("Estimated doc concentration:")
    println(topicModel.estimatedDocConcentration.toArray.mkString("\n"))
    println(s"Vocab size: ${topicModel.vocabSize}")
    
    topics
      .select($"topic", indexToWordTransform($"termIndices").as("terms"))
      .show(19, truncate = false)
    
//    topics
//      .select($"topic", $"termWeights")
//      .show(100, truncate = false)

    topics
      .select($"topic", explode(indexToWordTransform($"termIndices")).as("term"))
      .groupBy($"term").count()
      .filter($"count".geq(3))
      .orderBy($"count".desc)
      .show(25, truncate = false)
    
    val predictions = model.transform(pipeline1.fit(df).transform(df)).cache()
//    val predictions = model.transform(prepped.select($"description".as("text"))).cache()
    val roundUdf = udf((arr: Seq[Double]) => arr.map(x => roundNum(x, 2)))
    predictions
      .select($"filteredTokens", roundUdf($"topicDistribution"))
      .show(20, truncate = false)
    
//    val perplexity = topicModel.logPerplexity(predictions)
//    println(perplexity)
  }

  private def roundNum(num: Double, places: Int): BigDecimal = {
    BigDecimal(num).setScale(places, BigDecimal.RoundingMode.HALF_UP)
  }

  private def sampleDataSet(df: DataFrame, numPerCategory: Int): DataFrame = {
    val w = Window.partitionBy($"category").orderBy($"rng")
    val rows = df.withColumn("rn", row_number.over(w))
    rows.where($"rn".leq(numPerCategory)).drop("rn")
  }

}
