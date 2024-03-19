package impact.productclassifier.analysis

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{NGram, RegexTokenizer, StopWordsRemover, Tokenizer}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.storage.StorageLevel
import impact.productclassifier.App.spark
import VocabAnalyzer.cleanJsonString
import impact.productclassifier.DataLoader
import impact.productclassifier.feature.{SortedNGram, StopWords, Structs, TokenNormalizer, TokenizerFactory}
import impact.productclassifier.hierarchical.EnsembleUtil.vectorizer

import java.time.{Duration, Instant}

object VocabAnalyzer {

  import spark.implicits._

  private val tokenizer: RegexTokenizer = TokenizerFactory.tokenizer("words")

  private val normalizer: TokenNormalizer = new TokenNormalizer("words")
  
  def analyzeName(): Unit = {
    analyzeWords(readData($"name", 1000000))
  }

  def analyzeNameBigrams(): Unit = {
    analyzeBigrams(readData($"name", 50000))
  }
  
  def analyzeNameInfoGain(): Unit = {
    analyzeInformationGain(readData($"name", 100000))
  }

  def analyzeDescription(): Unit = {
    analyzeWords(readData($"description", 1000000))
  }

  def analyzeDescriptionBigrams(): Unit = {
    analyzeBigrams(readData($"description", 50000))
  }

  def analyzeDescriptionInfoGain(): Unit = {
    analyzeInformationGain(readData($"description", 500000))
  }

  def analyzeManufacturer(): Unit = {
    val df = readData($"manufacturer", 10000000)
    val words = df
      .select(lower($"words").as("words"), $"cat1")
//      .filter(length($"words").geq(3))
//      .filter(length(regexp_extract($"words", "([0-9]{3,})", 1)).leq(0))

//    val pipeline = new Pipeline().setStages(Array(
//      new Tokenizer().setInputCol("words").setOutputCol("tokens"),
//      new NGram().setInputCol("tokens").setOutputCol("ngrams").setN(2)
//    ))

    val wordCounts = words
//      .select(explode($"tokens").as("word"))
      .groupBy($"words").count()
      .orderBy($"count".desc).cache()
    showFrequencyDistribution(wordCounts)
    
    words
      .groupBy($"cat1", $"words").count()
      .orderBy($"count".desc)
      .show(200, truncate = false)

    println("Total manufacturers: " + wordCounts.agg(sum($"count")).collect()(0))
    println("Total unique manufacturers: " + wordCounts.count())
    println("Num generic manufacturers: " + wordCounts.filter($"count".geq(100000)).count())
    println("Num very common manufacturers: " + wordCounts.filter($"count".geq(10000)).count())
    println("Num common manufacturers: " + wordCounts.filter($"count".geq(1000)).count())
    println("Num uncommon manufacturers: " + wordCounts.filter($"count".leq(999)).count())
    println("Num rare manufacturers: " + wordCounts.filter($"count".leq(100)).count())
    println("-" * 100)
    println("Most common manufacturers:")
    wordCounts.sort($"count".desc).show(200, truncate = false)
//    println("Uncommon manufacturers:")
//    wordCounts
//      .filter($"count".leq(100))
//      .sort($"count".desc)
//      .show(100, truncate = false)
//    println("Least common manufacturers:")
//    wordCounts.sort($"count".asc).show(100, truncate = false)
    
//    val cond = $"words".contains("levi")
//      .or($"words".contains("carter"))
//      .or($"words".contains("nike"))
//      .or($"words".contains("puma"))
//      .or($"words".contains("adidas"))
//      .or($"words".contains("apple"))
//      .or($"manufacturer".contains(" inc"))
//      .or($"manufacturer".contains("victoria"))
//    wordCounts.filter(cond)
//      .show(1000, truncate = false)
  }
  
  private def readData(column: Column, numPerCategory: Int): DataFrame = {
    val data = DataLoader.readAllData(column, $"cat1", $"cat2")
    DataLoader.sampleDataSet(data.withColumn("rng", lit(0)), numPerCategory, 2)
      .select(column.as("words"), $"cat1", $"cat2")
      .repartition(256)
//    DataLoader.sampleDataSet(data.withColumn("rng", rand()), numPerCategory, 2)
//      .select(column.as("words"), $"cat1", $"cat2")
  }
  
  private def analyzeWords(df: DataFrame): Unit = {
    val pipeline = new Pipeline().setStages(Array(
      tokenizer,
      normalizer
    ))
    df.persist(StorageLevel.DISK_ONLY)
    val words = pipeline.fit(df).transform(df).select($"wordsTokens")
    println(Instant.now())
    println(s"Words partitions: ${words.rdd.getNumPartitions}")
    
//    benchmarkCountVectorizer(words)

    val wordCounts = words
      .select(explode(array_distinct($"wordsTokens")).as("word"))
      .groupBy($"word").count()
      .orderBy($"count".desc)
      .cache()

    println(s"wordCounts partitions: ${wordCounts.rdd.getNumPartitions}")
    
    df.unpersist()
    
    showFrequencyDistribution(wordCounts)
    println(Instant.now())

    println("Total words: " + wordCounts.agg(sum($"count")).collect()(0))
    // TODO: Fix by using head instead of limit
    println("Total occurrences of top 100 words: " + wordCounts.head(100).map(_.getAs[Long]("count")).sum)
    println("Total occurrences of top 1000 words: " + wordCounts.head(1000).map(_.getAs[Long]("count")).sum)
    println("Total occurrences of top 10000 words: " + wordCounts.head(10000).map(_.getAs[Long]("count")).sum)
    println("Total occurrences of top 20000 words: " + wordCounts.head(20000).map(_.getAs[Long]("count")).sum)
    println("Total unique words: " + wordCounts.count())
    println("Num universal words: " + wordCounts.filter($"count".geq(1000000)).count())
    println("Num generic words: " + wordCounts.filter($"count".geq(100000)).count())
    println("Num very common words: " + wordCounts.filter($"count".geq(10000)).count())
    println("Num common words: " + wordCounts.filter($"count".geq(1000)).count())
    println("Num uncommon words: " + wordCounts.filter($"count".leq(100)).count())
    println("Num rare words: " + wordCounts.filter($"count".leq(10)).count())
    println("Num singleton words: " + wordCounts.filter($"count".leq(1)).count())
    println("Most common words:")
    wordCounts.sort($"count".desc).show(200, truncate = false)
    println("Least common words:")
    wordCounts.sort($"count".asc).show(100, truncate = false)
//    println("Most common short words:")
//    wordCounts.filter(length($"word").leq(2)).sort($"count".desc).show(100, truncate = false)

    val weirds = wordCounts.filter(length(regexp_replace($"word", "\\A[a-zA-Z]+\\Z", "")).geq(1))
    println("Num weird universal words: " + weirds.filter($"count".geq(1000000)).count())
    println("Num weird generic words: " + weirds.filter($"count".geq(100000)).count())
    println("Num weird very common words: " + weirds.filter($"count".geq(10000)).count())
    println("Num weird common words: " + weirds.filter($"count".geq(1000)).count())
    println("Num weird uncommon words: " + weirds.filter($"count".leq(100)).count())
    println("Num weird rare words: " + weirds.filter($"count".leq(10)).count())
    println("Num weird singleton words: " + weirds.filter($"count".leq(1)).count())

//    println("Measurements:")
//    weirds
//      .filter(length(regexp_replace($"word", "\\A<.*>\\Z", "")).leq(0))
//      .sort($"count".desc)
//      .show(200, truncate = false)

    println("Weird Outliers:")
    weirds
      .filter($"count".between(1, 5))
      .show(200, truncate = false)
    println("Most common weird words:")
    weirds
      .filter(not($"word".contains("<")))
      .sort($"count".desc)
      .show(100, truncate = false)
    
    wordCounts.unpersist()
    
//    val wordFreqs = words
//      .select(explode(array_distinct($"tokens")).as("word"))
//      .groupBy($"word").count()
//      .cache()
//
//    println("-" * 100)
////    println("Most common words:")
////    wordFreqs.sort($"count".desc).show(100, truncate = false)
//    println("Num common words: " + wordFreqs.filter($"count".geq(1000)).count())
//    println("Num uncommon words: " + wordFreqs.filter($"count".leq(100)).count())
//    println("Num rare words: " + wordFreqs.filter($"count".leq(10)).count())
//    println("Num singleton words: " + wordFreqs.filter($"count".leq(1)).count())
//    println("Num words by length: ")
//    wordCounts.select($"word", length($"word").as("wordLen"))
//      .groupBy($"wordLen").count().orderBy("wordLen")
//      .show(100, truncate = false)
  }
  
  private def analyzeInformationGain(df: DataFrame): Unit = {
    val pipeline = new Pipeline().setStages(Array(
      tokenizer,
      normalizer
    ))
    val data = df
      .select($"words", concat_ws(" > ", $"cat1", $"cat2").as("category"))
      .orderBy($"category")
      .persist(StorageLevel.DISK_ONLY)
    
    val numRecords: Long = data.count()
    
    val words = pipeline.fit(data).transform(data).select($"wordsTokens", $"category")
    println(Instant.now())
    println(s"Words partitions: ${words.rdd.getNumPartitions}")

    var wordCategoryCounts = words
      .select(explode(array_distinct($"wordsTokens")).as("word"), $"category")
      .groupBy($"word", $"category").count()

    println(s"wordCounts partitions: ${wordCategoryCounts.rdd.getNumPartitions}")
    wordCategoryCounts = wordCategoryCounts.repartition(512).cache()
    
    val categoryIndices: Map[String, Int] = wordCategoryCounts
      .select($"category").distinct()
      .collect()
      .map(_.getAs[String]("category"))
      .sorted.zipWithIndex.toMap
    
    val categoryCounts: Array[Long] = data
      .groupBy($"category").count()
      .collect()
      .map(row => (row.getAs[String]("category"), row.getAs[Long]("count")))
      .sortBy(_._1).map(_._2)
    
    def computeInfoGain(counts: Seq[Row]): Double = {
      var sum = 0.0
      val countPerCategory: Map[Int, Long] = counts
        .map(row => (row.getAs[Int]("category"), row.getAs[Long]("count")))
        .toMap
      val wordFreq = countPerCategory.values.sum
      val wordNotPresentFreq = numRecords - wordFreq
      for (i <- categoryCounts.indices) {
        val catFreq: Long = categoryCounts(i)
        val wordCatFreq: Long = countPerCategory.getOrElse(i, 0)
        val wordCatNotPresentFreq: Long = catFreq - wordCatFreq
        if (wordCatNotPresentFreq > 0) {
          sum += wordCatNotPresentFreq * math.log(numRecords * wordCatNotPresentFreq.toDouble / (wordNotPresentFreq * catFreq))
        }
        if (wordCatFreq > 0) {
          sum += wordCatFreq * math.log(numRecords * wordCatFreq.toDouble / (wordFreq * catFreq)) 
        }
      }
      sum / numRecords
    } 
    
    val catIndexMap = typedLit(categoryIndices)
    val computeInfoGainUdf = udf((counts: Seq[Row]) => computeInfoGain(counts))
    val results = wordCategoryCounts
      .select($"word", catIndexMap($"category").as("category"), $"count")
      .select($"word", struct($"category", $"count").as("counts"))
      .groupBy($"word").agg(collect_list($"counts").as("countPerCat"), sum($"counts.count").as("count"))
      .select($"word", computeInfoGainUdf($"countPerCat").as("infoGain"), $"count")

    println("Most informative words:")
    results.orderBy($"infoGain".desc).show(200, truncate = false)
    println("Least informative words:")
    results.orderBy($"infoGain").show(100, truncate = false)
    println("Most common words:")
    results.orderBy($"count".desc).show(200, truncate = false)
    
    val topWordsByInfo = results.orderBy($"infoGain".desc).head(50000).map(_.getAs[String]("word")).toSet
    val topWordsByCount = results.orderBy($"count".desc).head(50000).map(_.getAs[String]("word")).toSet
    println(s"10K Intersection size: ${topWordsByInfo.take(10000).intersect(topWordsByCount.take(10000)).size}")
    println(s"20K Intersection size: ${topWordsByInfo.take(20000).intersect(topWordsByCount.take(20000)).size}")
    println(s"30K Intersection size: ${topWordsByInfo.take(30000).intersect(topWordsByCount.take(30000)).size}")
    println(s"40K Intersection size: ${topWordsByInfo.take(40000).intersect(topWordsByCount.take(40000)).size}")
    println(s"50K Intersection size: ${topWordsByInfo.take(50000).intersect(topWordsByCount.take(50000)).size}")
  }

  private def analyzeBigrams(df: DataFrame): Unit = {
    val pipeline = new Pipeline().setStages(Array(
      tokenizer,
      normalizer,
      new StopWordsRemover().setInputCol("wordsTokens").setOutputCol("filteredTokens")
        .setCaseSensitive(true).setStopWords(StopWords.FOR_SORTED_BIGRAMS),
      new SortedNGram().setInputCol("filteredTokens").setOutputCol("bigrams").setN(2)
    ))
    val words = pipeline.fit(df).transform(df).select($"bigrams").cache()

    val wordCounts = words
      .select(explode($"bigrams").as("bigram"))
      .groupBy($"bigram").count()
      .orderBy($"count".desc)
      .cache()

    words.unpersist()

    showFrequencyDistribution(wordCounts)

    println("Total words: " + wordCounts.agg(sum($"count")).collect()(0))
    println("Total occurrences of top 100 bigrams: " + wordCounts.limit(100).agg(sum($"count")).collect()(0))
    println("Total occurrences of top 1000 bigrams: " + wordCounts.limit(1000).agg(sum($"count")).collect()(0))
    println("Total occurrences of top 10000 bigrams: " + wordCounts.limit(10000).agg(sum($"count")).collect()(0))
    println("Total occurrences of top 20000 bigrams: " + wordCounts.limit(20000).agg(sum($"count")).collect()(0))
    println("Total unique bigrams: " + wordCounts.count())
    println("Num universal bigrams: " + wordCounts.filter($"count".geq(1000000)).count())
    println("Num generic bigrams: " + wordCounts.filter($"count".geq(100000)).count())
    println("Num very common bigrams: " + wordCounts.filter($"count".geq(10000)).count())
    println("Num common bigrams: " + wordCounts.filter($"count".geq(1000)).count())
    println("Num uncommon bigrams: " + wordCounts.filter($"count".leq(100)).count())
    println("Num rare bigrams: " + wordCounts.filter($"count".leq(10)).count())
    println("Num singleton bigrams: " + wordCounts.filter($"count".leq(1)).count())

    val weirds = wordCounts.filter(length(regexp_replace($"bigram", "\\A[a-zA-Z]+\\Z", "")).geq(1))
    println("Num weird universal bigrams: " + weirds.filter($"count".geq(1000000)).count())
    println("Num weird generic bigrams: " + weirds.filter($"count".geq(100000)).count())
    println("Num weird very common bigrams: " + weirds.filter($"count".geq(10000)).count())
    println("Num weird common bigrams: " + weirds.filter($"count".geq(1000)).count())
    println("Num weird uncommon bigrams: " + weirds.filter($"count".leq(100)).count())
    println("Num weird rare bigrams: " + weirds.filter($"count".leq(10)).count())
    println("Num weird singleton bigrams: " + weirds.filter($"count".leq(1)).count())

    println("Most common bigrams:")
    wordCounts.sort($"count".desc).show(1000, truncate = false)
//    println("Least common bigrams:")
//    wordCounts.sort($"count".asc).show(50, truncate = false)

//    println("Measurements:")
//    weirds
//      .filter(length(regexp_replace($"bigram", "\\A<.*>\\Z", "")).leq(0))
//      .sort($"count".desc)
//      .show(200, truncate = false)

//    println("Weird Outliers:")
//    weirds
//      .filter($"count".between(1, 5))
//      .show(200, truncate = false)
//    println("Most common weird words:")
//    weirds
//      .filter(not($"word".contains("<")))
//      .sort($"count".desc)
//      .show(100, truncate = false)

    wordCounts.unpersist()
  }
  
  def analyzeMaterial(): Unit = {
    val data = DataLoader.readAllData($"material", $"cat1")
      .filter($"material".isNotNull)
      .select(trim(regexp_replace($"material", "[0-9%]", "")).as("material"), $"cat1")
      .cache()
    
    val tokenizer = new RegexTokenizer().setInputCol("material").setOutputCol("tokens")
      .setPattern("[,/|:;]").setGaps(true)

    val wordCounts = tokenizer.transform(data)
      .select(explode($"tokens").as("word"))
      .select(trim($"word").as("word"))
      .groupBy($"word").count()
      .orderBy($"count".desc)
      .cache()

    println("Total words: " + wordCounts.agg(sum($"count")).collect()(0))
    println("Total occurrences of top 100 words: " + wordCounts.head(100).map(_.getAs[Long]("count")).sum)
    println("Total occurrences of top 200 words: " + wordCounts.head(200).map(_.getAs[Long]("count")).sum)
    println("Total occurrences of top 500 words: " + wordCounts.head(500).map(_.getAs[Long]("count")).sum)
    println("Total occurrences of top 1000 words: " + wordCounts.head(1000).map(_.getAs[Long]("count")).sum)
    println("Total unique words: " + wordCounts.count())
    println("Num very common words: " + wordCounts.filter($"count".geq(10000)).count())
    println("Num common words: " + wordCounts.filter($"count".geq(1000)).count())
    println("Num uncommon words: " + wordCounts.filter($"count".leq(100)).count())
    println("Num rare words: " + wordCounts.filter($"count".leq(10)).count())
    println("Num singleton words: " + wordCounts.filter($"count".leq(1)).count())
    println("Most common words:")
    wordCounts.sort($"count".desc).show(200, truncate = false)
    println("Least common words:")
    wordCounts.sort($"count".asc).show(100, truncate = false)
    
    tokenizer.transform(data)
      .select(explode($"tokens").as("word"), $"cat1")
      .groupBy($"cat1", $"word").count()
      .orderBy($"count".desc)
      .show(200, truncate = false)
  }
  
  def analyzeColor(): Unit = {
    val data = DataLoader.readAllData($"color", $"cat1")
      .filter(size($"color").geq(1))
      .repartition(512)
      .cache()

    val wordCounts = data
      .select(explode($"color").as("word"))
      .select(lower(trim($"word")).as("word"))
      .groupBy($"word").count()
      .orderBy($"count".desc)
      .cache()

    println("Total words: " + wordCounts.agg(sum($"count")).collect()(0))
    println("Total occurrences of top 100 words: " + wordCounts.head(100).map(_.getAs[Long]("count")).sum)
    println("Total occurrences of top 200 words: " + wordCounts.head(200).map(_.getAs[Long]("count")).sum)
    println("Total occurrences of top 500 words: " + wordCounts.head(500).map(_.getAs[Long]("count")).sum)
    println("Total occurrences of top 1000 words: " + wordCounts.head(1000).map(_.getAs[Long]("count")).sum)
    println("Total unique words: " + wordCounts.count())
    println("Num very common words: " + wordCounts.filter($"count".geq(10000)).count())
    println("Num common words: " + wordCounts.filter($"count".geq(1000)).count())
    println("Num uncommon words: " + wordCounts.filter($"count".leq(100)).count())
    println("Num rare words: " + wordCounts.filter($"count".leq(10)).count())
    println("Num singleton words: " + wordCounts.filter($"count".leq(1)).count())
    println("Most common words:")
    wordCounts.sort($"count".desc).show(300, truncate = false)
    println("Least common words:")
    wordCounts.sort($"count".asc).show(100, truncate = false)

    data
      .select(explode($"color").as("word"), $"cat1")
      .select(lower(trim($"word")).as("word"), $"cat1")
      .groupBy($"cat1", $"word").count()
      .orderBy($"count".desc)
      .show(200, truncate = false)
  }
  
  def analyzeTargetDemographics(): Unit = {
    val data = DataLoader.readAggregatedData($"targetDemographics", $"cat1")
      .select(cleanJsonString("targetDemographics"), $"cat1")
//      .filter(length($"targetDemographics").geq(3))
      .withColumn("targetDemographicsStruct", from_json($"targetDemographics", Structs.targetDemographicsStruct))
      .select(
        $"targetDemographicsStruct"("gender").as("gender"),
        $"targetDemographicsStruct"("ageGroup").as("ageGroup"),
        $"targetDemographicsStruct"("adult").as("adult"),
        $"cat1")
      .cache()
    
    data
      .select($"gender")
      .groupBy($"gender").count()
      .show()

    data
      .select($"ageGroup")
      .groupBy($"ageGroup").count()
      .show()

    data
      .select($"adult")
      .groupBy($"adult").count()
      .show()

    data
      .select($"gender", $"cat1")
      .groupBy($"cat1", $"gender").count()
      .show(50, truncate = false)

    data
      .select($"ageGroup", $"cat1")
      .groupBy($"cat1", $"ageGroup").count()
      .show(50, truncate = false)

    data
      .select($"adult", $"cat1")
      .groupBy($"cat1", $"adult").count()
      .show(50, truncate = false)
  }

  def analyzePricing(): Unit = {
    val roundToNearest = 1000.0
//    val price = round($"dollarPrice".divide(roundToNearest)).multiply(roundToNearest)
    
    def bucket(price: Double): Double = {
      if (price == 0) {
        0
      } else if (price <= 1) {
        1
      } else if (price <= 20) {
        math.round(price / 2) * 2.0
      } else if (price <= 100) {
        math.round(price / 10) * 10.0
      } else if (price <= 1000) {
        math.round(price / 100) * 100.0
      } else if (price <= 10000) {
        math.round(price / 1000) * 1000.0
      } else if (price <= 1e5) {
        math.round(price / 20000) * 20000.0
      } else if (price <= 1e6) {
        1e6
      } else {
        price
      }
    }
    
    val price = udf[Double, Double](bucket).apply($"dollarPrice")
    
    val data = DataLoader.readAllData($"dollarPrice", $"cat1")
//      .select(cleanJsonString("pricing"), $"cat1")
//      .withColumn("pricingStruct", from_json($"pricing", Structs.pricingStruct))
//      .select(
//        $"pricingStruct"("dollarPrice").as("dollarPrice"),
////        $"pricingStruct"("unitOfMeasure").as("unitOfMeasure"),
//        $"cat1")
      .select(price.as("price"), $"cat1")
      .repartition(512)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    
    val total = data.count()
    
    data
      .groupBy($"price").count()
      .select($"price", round($"count".divide(total).multiply(100), 6).as("fraction"))
      .orderBy($"price")
      .show(200, truncate = false)

    data
      .groupBy($"cat1", $"price").count()
      .select($"cat1", $"price", round($"count".divide(total).multiply(100), 6).as("fraction"))
      .orderBy($"price", $"fraction".desc)
      .show(1000, truncate = false)
  }

  private def cleanJsonString(colName: String): Column = {
    val column = col(colName)
    regexp_replace(trim(column, "\""), "\"\"", "\"").as(colName)
  }
  
  private def showFrequencyDistribution(wordCounts: DataFrame): Unit = {
    val x = wordCounts.agg(sum($"count").as("total")).select($"total")
    val total = x.collect()(0).getAs[Long]("total")
    val counts = wordCounts.limit(100000).collect()
    for (i <- 0 until 20) {
      val start = i * 5000
      val end = (i + 1) * 5000
      val subTotal = counts.slice(start, end).map(_.getAs[Long]("count")).sum
      val fraction = roundNum((100.0 * subTotal) / total, 3)
      println(f"$start%5s - $end%5s: $fraction%12s")
    }
  }
  
  private def benchmarkCountVectorizer(df: DataFrame): Unit = {
    val start: Instant = Instant.now()
    println(start)
    val model = vectorizer("words", 20000).fit(df)
    println(s"Fit CountVectorizer in ${Duration.between(start, Instant.now).getSeconds} seconds")
    for (i <- 0 until 25) {
      println(model.vocabulary.slice(i * 20, (i + 1) * 20).mkString(", "))
    }
  }

  private def roundNum(num: Double, places: Int): BigDecimal = {
    BigDecimal(num).setScale(places, BigDecimal.RoundingMode.HALF_UP)
  }
}
