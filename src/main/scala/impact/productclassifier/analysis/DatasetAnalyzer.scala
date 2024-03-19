package impact.productclassifier.analysis

import impact.productclassifier.DataLoader
import impact.productclassifier.feature.StopWords
import impact.productclassifier.taxonomy.{Category, Taxonomy}
import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws}

import scala.annotation.tailrec
import scala.collection.mutable

class DatasetAnalyzer(val spark: SparkSession) {

  import spark.implicits._

  private val tokenizer: RegexTokenizer = new RegexTokenizer()
    .setInputCol("words").setOutputCol("tokens")
    .setGaps(false).setPattern("\\w+")

  private val stopWordRemover: StopWordsRemover = new StopWordsRemover()
    .setInputCol("tokens").setOutputCol("filteredTokens")
    .setCaseSensitive(false).setStopWords(StopWords.STOP_WORDS)

  def analyze(): Unit = {
    val taxonomy = new Taxonomy().populate()
    showCategoryCounts(taxonomy)
    showMostCommonWords()
  }
  
  private def showMostCommonWords(): Unit = {
    val wordsColumn = concat_ws(" ", col("name"), col("description")).as("words")
    val words = DataLoader.readAllTrainingData().select(wordsColumn)
    
    val tokens = tokenizer.transform(words)
    val filteredTokens = stopWordRemover.transform(tokens).drop("words", "tokens")
    
    println("Num distinct tokens: " + filteredTokens.distinct().count())
    val tokensFlattened = filteredTokens.flatMap(row => row.getAs[mutable.ArraySeq[String]]("filteredTokens"))
    println("Total tokens: " + tokensFlattened.count())
    tokensFlattened
      .groupBy("value")
      .count()
      .sort(col("count").desc)
      .show(900, truncate = false)
  }

  private def showCategoryCounts(taxonomy: Taxonomy): Unit = {
    val categoryCounts: Array[(String, Long)] = DataLoader.readAllTrainingData()
      .groupBy(col("category"))
      .count()
      .collect()
      .map(row => (row.getAs[String]("category"), row.getAs[Long]("count")))

    categoryCounts.foreach(elem => updateCategoryCount(taxonomy.root, elem._1.split(" > "), elem._2))
    
    val numLevel1 = taxonomy.root.getCount
    val numLevel2 = taxonomy.root.getSubCategories
      .flatMap(_.getSubCategories)
      .map(_.getCount)
      .sum
    val numLevel3 = taxonomy.root.getSubCategories
      .flatMap(_.getSubCategories).flatMap(_.getSubCategories)
      .map(_.getCount)
      .sum
    val numLevel4 = taxonomy.root.getSubCategories
      .flatMap(_.getSubCategories).flatMap(_.getSubCategories).flatMap(_.getSubCategories)
      .map(_.getCount)
      .sum
    val numLevel5 = taxonomy.root.getSubCategories
      .flatMap(_.getSubCategories).flatMap(_.getSubCategories).flatMap(_.getSubCategories).flatMap(_.getSubCategories)
      .map(_.getCount)
      .sum

    println("Category stats:")
    println("Total count per level:")
    println(f"1: $numLevel1%,d")
    println(f"2: $numLevel2%,d")
    println(f"3: $numLevel3%,d")
    println(f"4: $numLevel4%,d")
    println(f"5: $numLevel5%,d")
    printCategoryStats(taxonomy.root)
  }
  
  @tailrec
  private def updateCategoryCount(category: Category, path: Array[String], count: Long): Unit = {
    category.incrementCount(count)
    if (!path.isEmpty && !category.isLeaf) {
      updateCategoryCount(category.getSubCategory(path.head), path.tail, count)
    }
  }
  
  private def printCategoryStats(category: Category): Unit = {
    if (category.level > 2) return
    
    val name = category.name
    val count = category.getCount
    val numLeadingSpaces = category.level * 35 - 20
    println((" " * numLeadingSpaces) + f"$name $count%,d")
    category
      .getSubCategories.sortBy(_.name)
      .foreach(printCategoryStats)
  }
  
  private def showTaxonomy(taxonomy: Taxonomy): Unit = {
    val numLevel1 = taxonomy.root.getSubCategories.size
    val numLevel2 = taxonomy.root.getSubCategories.map(_.getSubCategories.size).sum
    val numLevel3 = taxonomy.root.getSubCategories
      .flatMap(_.getSubCategories)
      .map(_.getSubCategories.size)
      .sum
    val numLevel4 = taxonomy.root.getSubCategories
      .flatMap(_.getSubCategories).flatMap(_.getSubCategories)
      .map(_.getSubCategories.size)
      .sum
    val numLevel5 = taxonomy.root.getSubCategories
      .flatMap(_.getSubCategories).flatMap(_.getSubCategories).flatMap(_.getSubCategories)
      .map(_.getSubCategories.size)
      .sum

    println("Taxonomy:")
    println("Num categories per level:")
    println(f"1: $numLevel1%,d")
    println(f"2: $numLevel2%,d")
    println(f"3: $numLevel3%,d")
    println(f"4: $numLevel4%,d")
    println(f"5: $numLevel5%,d")
    printCategory(taxonomy.root)
  }
  
  private def printCategory(category: Category): Unit = {
    if (category.level > 2) return
    
    val name = category.name
    val numDescendants = countDescendants(category)
    val numLeadingSpaces = category.level * 30 - 20 
    println((" " * numLeadingSpaces) + f"$name $numDescendants%,d")
    category
      .getSubCategories.sortBy(_.name)
      .foreach(printCategory)
  }
  
  private def countDescendants(category: Category): Int = {
    if (category.isLeaf) {
      return 1
    }
    category.getSubCategories.map(countDescendants).sum
  }
}
