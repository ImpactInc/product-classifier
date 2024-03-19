package impact.productclassifier.dataproc

import impact.productclassifier.DataLoader
import org.apache.spark.sql.{SaveMode, functions}
import org.apache.spark.sql.functions.{col, concat_ws, regexp_replace}
import impact.productclassifier.App.spark

import scala.collection.mutable

object DataAggregator {

  import spark.implicits._
  
  def summarizeCategoryFrequencies(): Unit = {
    val catCols = (1 to 7).map(i => col(s"cat$i"))
    val combinedCatCol = regexp_replace(concat_ws(" > ", catCols: _*), "(?: > )* > \\Z", "")
    val labelCounts: Array[(String, Long)] = DataLoader.readAllData(catCols: _*)
      .withColumn("category", combinedCatCol)
      .groupBy($"category").count()
      .collect()
      .map(row => (row.getAs[String]("category"), row.getAs[Long]("count")))
    
    val categoryCounts: mutable.Buffer[(String, Long)] = mutable.Buffer()
    val counts = labelCounts.map{case (cat, count) => (cat, count, cat.split(" > ").length)}
    
    for (i <- 1 to 7) {
      val x = counts
        .filter(_._3 >= i)
        .map{case (cat, count, _) => (cat.split(" > ", i + 1).slice(0, i).mkString(" > "), count)}
        .groupBy(_._1)
        .mapValues(_.map(_._2).sum)
        .toArray
      categoryCounts.appendAll(x)
    }
    
    println(categoryCounts.sortBy(_._1).mkString("", "\n", ""))
    
    spark.sparkContext.parallelize(labelCounts)
      .toDF("label", "count")
      .sort(functions.size(functions.split($"label", ">")), $"label")
      .coalesce(1)
      .write.mode(SaveMode.Overwrite).option("delimiter", ";").option("header", "true")
      .csv("gs://product-classifier/label-counts")

    spark.sparkContext.parallelize(categoryCounts)
      .toDF("category", "count")
      .sort(functions.size(functions.split($"category", ">")), $"category")
      .coalesce(1)
      .write.mode(SaveMode.Overwrite).option("delimiter", ";").option("header", "true")
      .csv("gs://product-classifier/category-counts")
  }
  
  def aggregateTrainingData(): Unit = {
    DataLoader.readProductDataDump()
      .drop("campaignId", "catalogId", "catalogItemId", "category")
      .repartition($"cat1", $"cat2", $"cat3") // minimizes number of files created
      .write
      .partitionBy("cat1", "cat2", "cat3")
      .parquet("gs://product-categorizer/data3.parquet")
  }
  
  def createValidationSet(): Unit = {
    val df = DataLoader.readAllData().cache()
    val (trainSet, testSet) = DataLoader.sampleAndSplitDataSet(df, Int.MaxValue, 7, 0.9)
    trainSet
      .repartition($"cat1", $"cat2", $"cat3")
      .write
      .partitionBy("cat1", "cat2", "cat3")
      .parquet("gs://product-classifier/train_data.parquet")
    testSet
      .repartition($"cat1", $"cat2", $"cat3")
      .write
      .partitionBy("cat1", "cat2", "cat3")
      .parquet("gs://product-classifier/test_data.parquet")
    df.unpersist()
  }
  
//  Commented out while not used due to large external dependency
//  def filterEnglishRecords(): Unit = {
//    val pipeline = new PretrainedPipeline("detect_language_21", lang = "xx")
//    val all = DataLoader.readAllData().repartition(512)
//    val results = pipeline.transform(all.withColumn("text", $"description"))
//      .withColumn("lang", element_at($"language.result", 1))
//      .drop("text")
//      .persist(StorageLevel.DISK_ONLY)
//    
//    val valid = results.filter($"lang".equalTo("en"))
//    val invalid = results.filter($"lang".notEqual("en"))
//    
//    println(valid.count())
//    println(invalid.count())
//
//    valid
//      .repartition($"cat1", $"cat2", $"cat3") // minimizes number of files created
//      .write
//      .partitionBy("cat1", "cat2", "cat3")
//      .parquet("gs://product-classifier/data_en.parquet")
//  }
}
