package impact.productclassifier.dataproc

import impact.productclassifier.DataLoader
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import impact.productclassifier.App.spark
import impact.productclassifier.feature.{TokenNormalizer, TokenizerFactory}

object DictionaryBuilder {

  import spark.implicits._
  
  private val stopWords: Array[String] = Array(
    "and", "are", "all", "any", "also", "available", "an", "a", "as", "at",
    "be", "been", "being", "but", "because", "by",
    "can",
    "do", "did", "doing",
    "from", "for",
    "get", "got",
    "has", "had", "have", "having", "here", "how",
    "its", "in", "is", "it", "if",
    "just",
    "made", "more", "most",
    "not", "none",
    "out", "our", "only", "other", "ours", "or", "of",
    "some", "such", "so", "same",
    "the", "this", "that", "them", "those", "they", "these", "their", "top", "than", "theirs", "to", "too",
    "until",
    "very",
    "with", "will", "when", "when", "which", "while", "we", "who", "whom", "what", "was", "were", "where", "why",
    "your", "you", "yours"
  )

  private val tokenizer: RegexTokenizer = TokenizerFactory.tokenizer("")
    .setInputCol("text").setOutputCol("tokens")
//    .setGaps(false).setPattern("[a-zA-Z0-9$%@\u0080¢£¥°º½¾`.\\-*÷×±øØµ\"',/]{3,30}")
//    .setToLowercase(false)
  
  private val normalizer: TokenNormalizer = new TokenNormalizer("tokens")

  private def applyRegex(column: Column, regex: String): Column = {
    length(regexp_extract(lower(column), regex, 1)).leq(0)
  }

  private val regex1 = "([aeiou]{5,})"
  private val regex2 = "([bcdfghjklmnpqrstvwxyz]{5,})"
//  private val regex3 = "(\\A[bcdfghjklmnpqrstvwxz]{2,3}\\z)"
  private val regex4 = "([0-9]{5,})"
  private val regex5 = "([0-9]+[a-z]+[0-9]+)"
  private val regex6 = "([a-z]+[0-9]+[a-z]+)"
  private val regex7 = "\\A((?:[a-z]+-)*[a-z]*[0-9]+(?:-[0-9]+)*)(?:-[a-z]+)*\\Z"
  private val regexFilter = (w: Column) => applyRegex(w, regex1) && applyRegex(w, regex2) && applyRegex(w, regex4) && 
    applyRegex(w, regex5) && applyRegex(w, regex6) && applyRegex(w, regex7)

  def buildDictionary(): Unit = {
    val data = DataLoader.readAggregatedData($"name", $"description", $"labels", $"cat1")
        .select(concat_ws(" ", $"name", $"description", $"labels").as("text"), $"cat1")
    val df = DataLoader.sampleDataSet(data.withColumn("rng", rand()), 100000, 1)
    
    val pipeline = new Pipeline().setStages(Array(
      tokenizer,
      normalizer,
      new StopWordsRemover().setInputCol("tokens").setOutputCol("filteredTokens")
        .setCaseSensitive(false).setStopWords(stopWords)
    ))
    
    val words = pipeline.fit(df).transform(df)
      .select(filter($"filteredTokens", regexFilter).as("tokens"))
    
    val wordCounts = words
      .select(explode($"tokens").as("word"))
      .groupBy($"word").count()
      .orderBy($"count")
      .cache()

    println("Total words: " + wordCounts.agg(sum($"count")).collect()(0))
    println("Total unique words: " + wordCounts.count())
    println("Num universal words: " + wordCounts.filter($"count".geq(1000000)).count())
    println("Num generic words: " + wordCounts.filter($"count".geq(100000)).count())
    println("Num very common words: " + wordCounts.filter($"count".geq(10000)).count())
    println("Num common words: " + wordCounts.filter($"count".geq(1000)).count())
    println("Num uncommon words: " + wordCounts.filter($"count".leq(100)).count())
    println("Num rare words: " + wordCounts.filter($"count".leq(10)).count())
    println("Num singleton words: " + wordCounts.filter($"count".leq(1)).count())
    
    val weirds = wordCounts.filter(length(regexp_replace($"word", "\\A[a-zA-Z]+\\Z", "")).geq(1))
    println("Num weird universal words: " + weirds.filter($"count".geq(1000000)).count())
    println("Num weird generic words: " + weirds.filter($"count".geq(100000)).count())
    println("Num weird very common words: " + weirds.filter($"count".geq(10000)).count())
    println("Num weird common words: " + weirds.filter($"count".geq(1000)).count())
    println("Num weird uncommon words: " + weirds.filter($"count".leq(100)).count())
    println("Num weird rare words: " + weirds.filter($"count".leq(10)).count())
    println("Num weird singleton words: " + weirds.filter($"count".leq(1)).count())
//    println("Most common words:")
//    wordCounts.sort($"count".desc).show(100, truncate = false)
//    println("Least common words:")
//    wordCounts.sort($"count".asc).show(200, truncate = false)
    println("Measurements:")
    weirds
      .filter(length(regexp_replace($"word", "\\A<.*>\\Z", "")).leq(0))
      .sort($"count".desc)
      .show(200, truncate = false)
//    println("Normal Outliers:")
//    wordCounts
//      .filter(length(regexp_replace($"word", "\\A[a-zA-Z]+\\Z", "")).leq(0))
//      .filter($"count".between(1, 5))
//      .show(200, truncate = false)
    println("Weird Outliers:")
    weirds
      .filter($"count".between(1, 5))
      .show(200, truncate = false)
    println("Most common weird words:")
    weirds
      .filter(not($"word".contains("<")))
      .sort($"count".desc)
      .show(200, truncate = false)
  }
}
