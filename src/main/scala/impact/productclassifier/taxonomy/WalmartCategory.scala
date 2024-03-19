package impact.productclassifier.taxonomy

import scala.collection.mutable

final class WalmartCategory(val name: String, val parent: Option[WalmartCategory], var gmcCategory: Option[String]) {

  private val childrenByName: mutable.Map[String, WalmartCategory] = mutable.Map.empty
  
  def addSubCategory(name: String, gmcCategory: Option[String]): WalmartCategory = {
    val entry: Option[WalmartCategory] = childrenByName.get(name)
    if (entry.nonEmpty) {
      val child = entry.get
      if (gmcCategory.nonEmpty) {
        if (child.gmcCategory.nonEmpty && !gmcCategory.get.equals(child.gmcCategory.get)) {
          throw new RuntimeException(s"$name is already mapped to ${child.gmcCategory.get}, not ${gmcCategory.get}")
        }
        child.gmcCategory = gmcCategory
      }
      child
    } else {
      val child = new WalmartCategory(name, Some(this), gmcCategory)
      childrenByName(name) = child
      child
    }
  }

  def getSubCategory(name: String): Option[WalmartCategory] = {
    childrenByName.get(name)
  }

  def getGmcMapping(walmartCategoryName: String): String = {
    var bestMapping: String = null
    var node = this
    for (name <- walmartCategoryName.split(">")) {
      val opt = node.getSubCategory(name)
      if (opt.isEmpty) {
        return bestMapping
      }
      node = opt.get
      if (node.gmcCategory.nonEmpty) {
        bestMapping = node.gmcCategory.get
      }
    }
    bestMapping
  }
}
