package impact.productclassifier.analysis

import impact.productclassifier.DataLoader
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{regexp_replace, trim}

class WalmartMappingAnalyzer(val spark: SparkSession) {

  import spark.implicits._
  
  
  def analyze(): Unit = {
    val mappings = DataLoader.readWalmartMappings()
    val wCats = (0 until 3).map(i => trim($"wCats"(i)).alias(s"wCat${i + 1}"))
    val gCats = (0 until 8).map(i => trim($"gCats"(i)).alias(s"gCat${i + 1}"))
    
    
    mappings
//      .withColumn("wCats", split(col("walmart"), ">"))
//      .withColumn("gCats", split(col("google"), ">"))
//      .select(wCats ++ gCats: _*)
      .select(
        regexp_replace($"walmart", "[,']", "").as("walmart"),
        regexp_replace($"google", "[,']", "").as("google")
      )
      .groupByKey(row => row.getAs[String]("walmart"))
      .mapGroups((key, rows) => 
        (key, rows
          .map(_.getAs[String]("google"))
          .toSet
        ))
//      .mapValues()
//      .count()
//      .orderBy($"wCat2")
      .show(1000, truncate = false)
    
  }
}
