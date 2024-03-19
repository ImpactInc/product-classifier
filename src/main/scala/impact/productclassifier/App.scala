package impact.productclassifier

import impact.productclassifier.analysis.VocabAnalyzer
import impact.productclassifier.dataproc.DataAggregator
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{from_json, length, lit, lower}
import org.apache.spark.storage.StorageLevel
import DataLoader.cleanJsonString
import impact.productclassifier.feature.{Structs}

import java.time.{Duration, Instant}
import scala.language.postfixOps


object App {
  import org.apache.spark.sql.SparkSession

  val spark: SparkSession = SparkSession.builder()
    .appName("ProductClassifier")
//    .master("local[8]")
    .config("spark.sql.sources.parallelPartitionDiscovery.threshold", 128)
    .config("spark.network.timeout", 300)
    .config("spark.dynamicAllocation.enabled", value = false)
    .getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")
  spark.sparkContext.setCheckpointDir("gs://product-classifier/checkpoints")
  
  val isOnDataproc = true
  val GCS_BUCKET: String = "gs://wikus-product-categorizer"
  
  
  def main(args: Array[String]): Unit = {
    val start: Instant = Instant.now()
    println(start)
    println(spark.sparkContext.getConf.get("spark.driver.memory"))
    println(spark.sparkContext.getConf.get("spark.executor.memory"))
    println(spark.sparkContext.getConf.get("spark.network.timeout"))
    println(spark.sparkContext.getConf.get("spark.default.parallelism"))
    println(spark.sparkContext.getConf.get("spark.sql.shuffle.partitions"))
    
    
    
//    Tester.run2()
//    ModelTrainer.createName2VecModel()

//    val pipeline = new PretrainedPipeline("detect_language_21", lang = "xx")
//    println(pipeline.model.stages.mkString(", "))
//
//    val df = DataLoader.readAllData($"description")
//      .limit(1000000)
//      .select($"description".as("text"))
//      .repartition(512)
//      .persist(StorageLevel.DISK_ONLY)
//    println(s"Num partitions: ${df.rdd.getNumPartitions}")
//    val result = pipeline.transform(df)
//      .select("text", "language.result")
//      .select(element_at($"result", 1).as("lang")).cache()
//    println(Instant.now())
//    println(result.count())
//    println(result.filter($"lang".notEqual("en")).count())
    
//    result
//      .groupBy($"lang").count()
//      .orderBy($"count".desc)
//      .show(100, truncate = false)
    
//    val features: Array[String] = Array(
//      "physicalAttributes", 
//      "targetDemographics",
//    )
//    val cats = (1 to 1).map(i => col(s"cat$i"))
//    val df = DataLoader.readAggregatedData(features.map(col) ++ cats: _*)
    
//    def cleanJsonString(colName: String): Column = {
//      val column = col(colName)
//      regexp_replace(trim(column, "\""), "\"\"", "\"").as(colName)
//    }
//    val df2 = df
//      .select(cleanJsonString("targetDemographics"), cleanJsonString("physicalAttributes"))
//      .filter(length($"physicalAttributes").geq(3) || length($"targetDemographics").geq(3))
//      .withColumn("physicalAttributesStruct", from_json($"physicalAttributes", Structs.physicalAttributesStruct))
//      .withColumn("targetDemographicsStruct", from_json($"targetDemographics", Structs.targetDemographicsStruct))
    
//    val fields = Array(
////      "color",
//      "material",
////      "pattern",
//      "size"
//    )
    
//    for (f <- fields) {
//      var column = lower($"physicalAttributesStruct"(f))
//      var condition = column.isNotNull.and(column.notEqual(lit("null")))
//      if (f.equals("color")) {
//        column = transform($"physicalAttributesStruct"(f), x => regexp_replace(lower(x), "grey", "gray"))
//        condition = size(col(f)).geq(1)
//      } else if (f.equals("material")) {
//        column = regexp_replace(column, "[0-9%\\s]", "")
//        column = regexp_replace(column, "[,/]", "+")
//        condition = col(f).isNotNull.and(col(f).notEqual(lit("null")))
//      } else if (f.equals("size")) {
//        column = regexp_replace(column, "small", "s")
//        column = regexp_replace(column, "medium", "m")
//        column = regexp_replace(column, "large", "l")
//        condition = col(f).isNotNull.and(col(f).notEqual(lit("null")))
//      }
//      val result = df2
//        .select(column.as(f))
//        .filter(condition)
//        .groupBy(col(f)).count().cache()
//      println(f"$f%-20s: ${result.count()}%10s")
//      result.orderBy($"count".desc).show(200, truncate = false)
//    }
//    for (f <- Structs.targetDemographicsStruct.fields.map(_.name)) {
//      val column = $"targetDemographicsStruct"(f)
//      val condition = column.isNotNull.and(column.notEqual(lit("null")))
//      val num = df2.filter(condition).count()
////      println(f"$f%-20s: $num%10s")
//    }

    println(s"Finished in ${Duration.between(start, Instant.now).getSeconds} seconds")
    spark.close()
  }
}
