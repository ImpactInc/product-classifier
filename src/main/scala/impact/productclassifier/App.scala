package impact.productclassifier

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
    
    Tester.run()
//    ModelTrainer.createName2VecModel()

    println(s"Finished in ${Duration.between(start, Instant.now).getSeconds} seconds")
    spark.close()
  }
  
  private def testLanguageDetection(): Unit = {
    // Commented out due to large dependency
    
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
  }
}
