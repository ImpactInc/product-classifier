package impact.productclassifier.taxonomy

import impact.productclassifier.App.spark

class WalmartTaxonomy {

  val root: WalmartCategory = new WalmartCategory("root", None, None)

  def populate(): this.type = {
    val lines = readLinesFromCsv()
    val categories: Seq[(Array[String], String)] = lines
      .map(_.toLowerCase.split(";"))
      .map(arr => (arr(0).split(">"), arr(1).toLowerCase))
      .filter(_._1.length > 1)
      .filter(_._2.split(">").length > 1)

    categories.foreach(elem => addCategory(elem._1, elem._2))
    this
  }

  private def addCategory(path: Seq[String], gmcCategory: String): Unit = {
    var node = root
    for (name <- path.slice(0, path.length - 1)) {
      node = node.addSubCategory(name, None)
    }
    node.addSubCategory(path.last, Some(gmcCategory))
  }

  private def readLinesFromCsv(): Seq[String] = {
    spark.read
      .text("gs://product-categorizer/walmart-to-gmc-mappings.csv")
      .collect()
      .map(_.getAs[String]("value"))
      .tail
//    try {
////      val path = Paths.get(System.getenv("HOME") + "/Documents/PDS/Taxonomy/walmart-to-gmc-mappings.csv")
//      Files.readAllLines(Paths.get("gs://product-categorizer/walmart-to-gmc-mappings.csv")).asScala.tail
//    } catch {
//      case _: IOException =>
//        Collections.emptyList[String].asScala
//    }
  }
}
