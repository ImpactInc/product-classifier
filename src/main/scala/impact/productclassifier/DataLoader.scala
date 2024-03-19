package impact.productclassifier

import impact.productclassifier.feature.Structs
import impact.productclassifier.misc.GoogleTaxonomy
import impact.productclassifier.taxonomy.WalmartTaxonomy
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.storage.StorageLevel
import App.{GCS_BUCKET, isOnDataproc, spark}

import java.io.File
import java.nio.file.{Path, Paths}
import scala.util.Random

object DataLoader {

  import spark.implicits._

  private val categoryIdsByName: Map[String, Int] = GoogleTaxonomy.getGoogleCategoryMap.idsByName.toMap
  private val rootCategories: Set[String] = GoogleTaxonomy.getGoogleCategoryMap.levelOne.keySet.toSet

  def prepareTrainingData(df: DataFrame, maxCategoryDepth: Int): DataFrame = {
    val nameToIdMap: Column = typedLit(categoryIdsByName)
    val categoryIds = (1 until maxCategoryDepth + 1)
      .map(i => nameToIdMap(col(s"cat$i")).as(s"cat$i"))
    df.select($"name" +: $"manufacturer" +: $"description" +: $"labels" +: categoryIds: _*)
  }

  def readAllTrainingData(): DataFrame = {
    val files: Array[String] = new File(getFilePath(f"Documents/PDS/Taxonomy/training-data").toString)
      .listFiles()
      .map(_.getPath)
      .filter(_.endsWith(".csv"))
    val df = readTrainingData(Random.shuffle(files.toList).toArray)

    val gzFiles: Array[String] = new File(getFilePath(f"Documents/PDS/Taxonomy/training-data/all-walmart").toString)
      .listFiles()
      .map(_.getPath)
      .filter(_.endsWith(".csv.gz"))
    val df2 = readGzippedTrainingData(Random.shuffle(gzFiles.toList).toArray)
    
    df.union(df2)
  }
  
  def readAllData(cols: Column*): DataFrame = {
    val df = spark.read.parquet("gs://product-classifier/train_data.parquet") 
    if (cols.nonEmpty) {
      df.select(cols: _*)
    } else {
      df
    }
  }
  
  def readProductDataDump(): DataFrame = {
    val walmartTaxonomy: WalmartTaxonomy = new WalmartTaxonomy().populate()
    val columns = Array(
      $"CampaignId".as("campaignId"),
      $"CatalogId".as("catalogId"),
      $"CatalogItemId".as("catalogItemId"),
      concat_ws(" ", $"Name", $"ParentName").as("name"),
      concat_ws(" ", $"Description", $"Labels").as("description"),
      $"Manufacturer".as("manufacturer"),
      lower($"Category").as("category"),
      from_json(cleanJsonString("Attributes"), Structs.attributesStruct).as("attributes")
    )
    spark.sparkContext.parallelize(Array("")).toDF()
    val df = spark.read.option("pathGlobFilter", "*.parquet")
      .parquet("gs://pds-dumps/ProductData")
      .select(columns: _*)
      .withColumn("physicalAttributes", $"attributes"("physicalAttributes"))
      .withColumn("color", $"physicalAttributes"("color"))
      .withColumn("material", $"physicalAttributes"("material"))
      .withColumn("apparelSizeSystem", $"physicalAttributes"("apparelSizeSystem"))
      .withColumn("apparelSizeType", $"physicalAttributes"("apparelSizeType"))
      .withColumn("targetDemographics", $"attributes"("targetDemographics"))
      .withColumn("gender", $"targetDemographics"("gender"))
      .withColumn("ageGroup", $"targetDemographics"("ageGroup"))
      .withColumn("pricing", $"attributes"("pricing"))
      .withColumn("dollarPrice", $"pricing"("dollarPrice"))
      .withColumn("unitOfMeasure", $"pricing"("unitOfMeasure"))
      .withColumn("taxonomy", $"attributes"("taxonomy"))
      .withColumn("filterCategory", lower($"taxonomy"("filterCategory")))
      .filter(length($"filterCategory").geq(3) || $"campaignId".equalTo(lit(9383)))
      .drop("physicalAttributes", "targetDemographics", "pricing", "taxonomy")
      .na.fill("null", Array("material", "apparelSizeSystem", "apparelSizeType", "gender", "ageGroup", "unitOfMeasure"))
      .persist(StorageLevel.DISK_ONLY)
    println(s"Num partitions: ${df.rdd.getNumPartitions}")

    val hasGmcCategory = df.filter(length($"filterCategory").geq(3))
    val walmartData = df.filter($"campaignId".equalTo(lit(9383)))
    println(hasGmcCategory.count())
    println(walmartData.count())

    val categories: Array[String] = walmartData
      .select($"category").distinct().collect()
      .map(_.getAs[String]("category"))
      .filter(_ != null)
    val categoryMap: Map[String, String] = categories
      .map(cat => (cat, walmartTaxonomy.root.getGmcMapping(cat)))
      .filter(_._2 != null).toMap
    println(s"---------------------------- ${categoryMap.size}")
    val categoryTranslator = typedLit(categoryMap)

    val mappedWalmartData = walmartData
      .drop($"filterCategory")
      .withColumn("filterCategory", categoryTranslator($"category"))
      .filter($"filterCategory".isNotNull)
    println(mappedWalmartData.count())

    var allData = hasGmcCategory.union(mappedWalmartData)
      .drop($"category")
      .withColumnRenamed("filterCategory", "category")

    val splitCat = split(col("category"), " > ")
    val splitCatCols = (0 until 7).map(i => (s"cat${i + 1}", splitCat(i))).toMap
    for ((name, column) <- splitCatCols) {
      allData = allData.withColumn(name, column)
    }
    allData
  }
  
  def readNonWalmartData(): DataFrame = {
    val files: Array[String] = new File(getFilePath(f"Documents/PDS/Taxonomy/training-data").toString)
      .listFiles()
      .map(_.getPath)
      .filter(_.endsWith(".csv"))
    readTrainingData(Random.shuffle(files.toList).toArray)
  }

  private def readTrainingData(paths: Array[String]): DataFrame = {
    try {
      val df = spark.read
        .option("delimiter", ";")
        .option("header", "true")
        .csv(paths: _*)
      preprocess(df)
    } catch {
      case e: Exception =>
        println(paths.mkString("\n"))
        throw e
    }
  }

  private def readGzippedTrainingData(paths: Array[String]): DataFrame = {
    try {
      val df = spark.read
        .option("delimiter", ";")
        .option("header", "true")
        .option("compression", "gzip")
        .csv(paths: _*)
      preprocess(df)
    } catch {
      case e: Exception =>
        println(paths.mkString("\n"))
        throw e
    }
  }
  
  private def preprocess(df: DataFrame): DataFrame = {
    val rawColumns = df.columns.diff(Array("name", "parentName", "description", "labels", "gmcCategory")).map(col)
    val nameCol = concat_ws(" ", $"name", $"parentName").as("name")
    val descCol = concat_ws(" ", $"description", $"labels").as("description")
    val catCol = lower($"gmcCategory").as("category")
    val splitCat = split(col("category"), " > ")
    val splitCatCols = (0 until 7).map(i => (s"cat${i + 1}", splitCat(i))).toMap
    var result = df
    for ((name, column) <- splitCatCols) {
      result = result.withColumn(name, column)
    }
    result.na.fill("", Seq("name", "parentName", "manufacturer", "description", "labels"))
      .select(Array(nameCol, descCol, catCol) ++ rawColumns: _*)
      .filter(col("cat1").isInCollection(rootCategories))
  }

  def getFilePath(fileLocation: String): Path = {
    var location = fileLocation.replace("~", "")
    if (!location.startsWith("/")) {
      location = "/" + location
    }
    val home = System.getenv("HOME")
    if (!location.startsWith(home)) {
      location = home + location
    }
    Paths.get(location)
  }

  def sampleDataSet(df: DataFrame, numPerCategory: Int, trainFrac: Double): (DataFrame, DataFrame) = {
    val sizes: Map[Int, Int] = df.groupBy($"cat1").count().collect()
      .map(row => (row.getAs[Int]("cat1"), row.getAs[Long]("count")))
      .map { case (id, count) => (id, Math.min(count.toInt, numPerCategory)) }
      .toMap

    val trainingSizeByCategory = round(typedLit(sizes).apply($"cat1").multiply(trainFrac))

    val w = Window.partitionBy($"cat1").orderBy($"cat1")
    val rows = df.withColumn("rn", row_number.over(w))
    val trainingSet = rows.where($"rn".leq(trainingSizeByCategory)).drop("rn")
    val validationSet = rows.where($"rn".between(trainingSizeByCategory + 1, numPerCategory)).drop("rn")
    (trainingSet, validationSet)
  }

  def sampleDataSet(df: DataFrame, numPerCategory: Int): DataFrame = {
    var sampleDf = df.withColumn("rng", rand())
    for (i <- 0 until 7) {
      sampleDf = sampleDataSet(sampleDf, numPerCategory, 7 - i)
    }
    sampleDf.drop($"rng")
  }

  def sampleDataSet(df: DataFrame, numPerCategory: Int, categoryDepth: Int): DataFrame = {
    val catCols = (1 to categoryDepth).map(i => col(s"cat$i"))
    val combinedCatCol = regexp_replace(concat_ws(" > ", catCols: _*), " > \\Z", "")
    val sizes: Map[String, Int] = getCategoryFrequencyMap(categoryDepth, numPerCategory)
    
    println(sizes.toArray.sortBy(_._1).map(x => f"${x._1}%-55s -> ${x._2}%8s").mkString("\n"))

    val sizeByCategory = round(typedLit(sizes).apply($"combinedCatCol"))

    val w = Window.partitionBy($"combinedCatCol").orderBy($"rng")
    df
      .withColumn("combinedCatCol", combinedCatCol)
      .withColumn("rn", row_number.over(w))
      .where($"rn".leq(sizeByCategory)).drop("rn")
      .drop("combinedCatCol")
  }
  
  def sampleAndSplitDataSet(df: DataFrame, numPerCategory: Int, categoryDepth: Int, trainFrac: Double): (DataFrame, DataFrame) = {
    val catCols = (1 to categoryDepth).map(i => col(s"cat$i"))
    val combinedCatCol = regexp_replace(concat_ws(" > ", catCols: _*), "(?: > )* > \\Z", "")
    val sizes: Map[String, Int] = getLabelFrequencyMap(categoryDepth, numPerCategory)
    val sizeByCategory = round(typedLit(sizes).apply($"combinedCatCol"))
    val w = Window.partitionBy($"combinedCatCol").orderBy(lit(0))
    val df2 = df
//      .withColumn("rng", rand())
      .withColumn("combinedCatCol", combinedCatCol)
      .withColumn("rn", row_number.over(w))
    val train = df2
      .where($"rn".lt(round(sizeByCategory.multiply(trainFrac))))
      .drop("combinedCatCol", "rn")
    val test = df2
      .where($"rn".between(round(sizeByCategory.multiply(trainFrac)), sizeByCategory))
      .drop("combinedCatCol", "rn")
    (train, test)
  }
  
  def getCategoryFrequencyMap(maxDepth: Int, maxPerCategory: Int): Map[String, Int] = {
    spark.read.option("delimiter", ";").option("header", "true")
      .csv("gs://product-classifier/category-counts")
      .select($"category", $"count".cast(IntegerType))
      .filter(size(split($"category", ">")) <= maxDepth)
      .collect()
      .map(row => (row.getAs[String]("category"), row.getAs[Int]("count")))
      .map { case (category, count) => (category, Math.min(count, maxPerCategory)) }
      .groupBy(_._1).mapValues(_.map(_._2).sum)
  }

  def getLabelFrequencyMap(maxDepth: Int, maxPerCategory: Int): Map[String, Int] = {
    spark.read.option("delimiter", ";").option("header", "true")
      .csv("gs://product-classifier/label-counts")
      .select($"label".as("category"), $"count".cast(IntegerType))
      .filter(size(split($"category", ">")) <= maxDepth)
      .collect()
      .map(row => (row.getAs[String]("category"), row.getAs[Int]("count")))
      .map { case (category, count) => (category, Math.min(count, maxPerCategory)) }
      .groupBy(_._1).mapValues(_.map(_._2).sum)
  }
  
  def readWalmartMappings(): DataFrame = {
    spark.read
      .option("delimiter", ";")
      .option("header", "true")
      .csv(getFilePath("Documents/PDS/Taxonomy/walmart-to-gmc-mappings.csv").toString)
      .select($"Walmart Category".as("walmart"), $"Google Category".as("google"))
  }
  
  def readAggregatedData(cols: Column*): DataFrame = {
    val pathPrefix = if (isOnDataproc) GCS_BUCKET else getFilePath("Documents/PDS/Taxonomy/training-data").toString 
    var df = spark.read.parquet(pathPrefix + "/aggregated/data.parquet")
    if (cols.nonEmpty) {
      val physical = Array($"color", $"material", $"apparelSizeSystem", $"apparelSizeType")
      if (cols.intersect(physical).nonEmpty) {
        df = df
          .withColumn("physicalAttributesStruct", from_json(cleanJsonString("physicalAttributes"), Structs.physicalAttributesStruct))
          .withColumn("color", $"physicalAttributesStruct"("color"))
          .withColumn("material", $"physicalAttributesStruct"("material"))
          .withColumn("apparelSizeSystem", $"physicalAttributesStruct"("apparelSizeSystem"))
          .withColumn("apparelSizeType", $"physicalAttributesStruct"("apparelSizeType"))
          .drop($"physicalAttributesStruct")
          .na.fill("null", Array("material", "apparelSizeSystem", "apparelSizeType"))
      }
      val demographic = Array($"gender", $"ageGroup")
      if (cols.intersect(demographic).nonEmpty) {
        df = df
          .withColumn("targetDemographicsStruct", from_json(cleanJsonString("targetDemographics"), Structs.targetDemographicsStruct))
          .withColumn("gender", $"targetDemographicsStruct"("gender"))
          .withColumn("ageGroup", $"targetDemographicsStruct"("ageGroup"))
          .drop($"targetDemographicsStruct")
          .na.fill("null", Array("gender", "ageGroup"))
      }
      val pricing = Array($"dollarPrice", $"unitOfMeasure")
      if (cols.intersect(pricing).nonEmpty) {
        df = df
          .withColumn("pricingStruct", from_json(cleanJsonString("pricing"), Structs.pricingStruct))
          .withColumn("dollarPrice", $"pricingStruct"("dollarPrice"))
          .withColumn("unitOfMeasure", $"pricingStruct"("unitOfMeasure"))
          .drop($"pricingStruct")
          .na.fill("null", Array("unitOfMeasure"))
      }
      df.select(cols: _*)
    } else df
  }

  def cleanJsonString(colName: String): Column = {
    val column = col(colName)
    regexp_replace(trim(column, "\""), "\"\"", "\"").as(colName)
  }
}
