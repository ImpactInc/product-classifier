package impact.productclassifier.dataproc

import impact.productclassifier.taxonomy.WalmartTaxonomy
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{lower, typedLit}
import impact.productclassifier.App.spark
import impact.productclassifier.DataLoader.getFilePath

import java.io.File

object WalmartCatalogProcessor {

  import spark.implicits._

  def processAndAggregateWalmartData(): Unit = {
    val walmartTaxonomy: WalmartTaxonomy = new WalmartTaxonomy().populate()
    val walmartData = readWalmartData()
    val categories: Array[String] = walmartData
      .select(lower($"gmcCategory").as("category"))
      .distinct()
      .collect()
      .map(_.getAs[String]("category"))
      .filter(_ != null)
    
    val categoryMap: Map[String, String] = categories
      .map(cat => (cat, walmartTaxonomy.root.getGmcMapping(cat)))
      .filter(_._2 != null)
      .toMap
    
    val categoryTranslator = typedLit(categoryMap)
    
    walmartData
      .withColumn("category", categoryTranslator(lower($"gmcCategory")))
      .drop($"gmcCategory")
      .withColumnRenamed("category", "gmcCategory")
      .filter($"gmcCategory".isNotNull)
      .write
      .option("delimiter", ";")
      .option("header", "true")
      .option("compression", "gzip")
      .csv(getFilePath(f"Documents/PDS/Taxonomy/training-data/all-walmart").toString)
  }
  
  private def readWalmartData(): DataFrame = {
    val files: Array[String] = new File(getFilePath(f"Documents/PDS/Taxonomy/training-data/walmart-catalogs").toString)
      .listFiles()
      .map(_.getPath)
      .filter(_.endsWith(".csv"))
    readWalmartData(files)
  }

  private def readWalmartData(paths: Array[String]): DataFrame = {
    try {
      spark.read
        .option("delimiter", ";")
        .option("header", "true")
        .csv(paths: _*)
    } catch {
      case e: Exception =>
        println(paths.mkString("\n"))
        throw e
    }
  }
}
