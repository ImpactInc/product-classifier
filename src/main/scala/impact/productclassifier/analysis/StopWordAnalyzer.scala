package impact.productclassifier.analysis

import impact.productclassifier.DataLoader
import impact.productclassifier.feature.{StopWords, TokenNormalizer, TokenizerFactory}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.functions.{explode, length, rand, regexp_replace}
import org.apache.spark.sql.{Column, DataFrame}
import impact.productclassifier.App.spark

object StopWordAnalyzer {

  import spark.implicits._

  private val tokenizer: RegexTokenizer = TokenizerFactory.tokenizer("words")

  private val normalizer: TokenNormalizer = new TokenNormalizer("words")

  // TODO: Find words that are common across product categories
  
  def analyzeName(): Unit = {
    analyzeWords(readData($"name", 100000))
  }
  
  private def analyzeWords(df: DataFrame): Unit = {
    val pipeline = new Pipeline().setStages(Array(
      tokenizer,
      normalizer,
      new StopWordsRemover().setInputCol("wordsTokens").setOutputCol("wordsTokensFiltered")
        .setCaseSensitive(false).setStopWords(StopWords.EMPIRICAL)
    ))
    
    val words = pipeline.fit(df).transform(df)
    
    val wordCounts = words
      .select(explode($"wordsTokensFiltered").as("word"), $"cat1")
      .filter(length(regexp_replace($"word", "\\A[a-zA-Z]+\\Z", "")).leq(0))
      .groupBy($"word", $"cat1").count()
      .orderBy($"count".desc)
      .limit(19 * 50).cache()
      
    wordCounts
      .orderBy($"word", $"cat1")
      .show(500, truncate = false)
  }

  private def readData(column: Column, numPerCategory: Int): DataFrame = {
    val data = DataLoader.readAggregatedData(column, $"cat1")
    DataLoader.sampleDataSet(data.withColumn("rng", rand()), numPerCategory, 1)
      .select(column.as("words"), $"cat1")
  }
}
