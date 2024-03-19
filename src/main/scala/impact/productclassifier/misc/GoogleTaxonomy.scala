package impact.productclassifier.misc

import impact.productclassifier.App.spark

object GoogleTaxonomy {
  
  def readGoogleCategories: Seq[Category] = {
    readFromCsv
      .map(_.split(";"))
      .map(values => new Category(values.head.toInt, values.tail))
      .sortBy(_.id)
  }

  def getGoogleCategoryMap: CategoryMap = {
    val categoryMap = new CategoryMap
    readFromCsv
      .map(_.split(";"))
      .foreach(values => categoryMap.add(values.head.toInt, values.tail))
    categoryMap
  }

  private def readFromCsv: Seq[String] = {
    spark.read
      .text("gs://product-categorizer/google_taxonomy.csv")
      .collect()
      .map(_.getAs[String]("value"))
      .tail
//    var lines: util.List[String] = null
//    try
//      //            Path path = Paths.get(System.getenv("HOME") + "/Documents/PDS/Taxonomy/google_taxonomy.csv");
//      //            lines = Files.readAllLines(path);
//      lines = Files.readAllLines(Paths.get("gs://product-categorizer/google_taxonomy.csv"))
//    catch {
//      case e: IOException =>
//        return Collections.emptyList
//    }
//    lines.remove(0)
//    lines
  }
}