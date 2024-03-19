package impact.productclassifier.taxonomy

import scala.collection.mutable

final class Category(val name: String, val id: Int, val parent: Option[Category]) {

  val level: Int = getLevel
  private val idMap: mutable.Map[Int, Category] = mutable.Map.empty
  private val nameMap: mutable.Map[String, Category] = mutable.Map.empty
  private var count: Long = 0
  
  def addSubCategory(name: String, id: Int): Category = {
    val entry: Option[Category] = idMap.get(id)
    if (entry.isEmpty) {
      val subCategory = new Category(name, id, Some(this));
      idMap(id) = subCategory
      nameMap(name) = subCategory
      subCategory
    } else {
      entry.get
    }
  }
  
  def getSubCategories: Seq[Category] = {
    idMap.values.toIndexedSeq
  }
  
  def getSubCategory(name: String): Category = {
    nameMap(name)
  }

  def getSubCategoryName(id: Int): String = {
    idMap(id).name
  }
  
  def getSubCategoryIdIndexMap: Map[Int, Int] = {
    getSubCategories
      .sortBy(_.name).map(_.id)
      .zipWithIndex.toMap
  }

  def getSubCategoryNameIndexMap: Map[String, Int] = {
    getSubCategories
      .sortBy(_.name).map(_.name)
      .zipWithIndex.toMap
  }
  
  def isRoot: Boolean = {
    parent.isEmpty
  }
  
  def isLeaf: Boolean = {
    idMap.isEmpty
  }
  
  def getPath: String = {
    if (parent.nonEmpty && parent.get.isRoot) {
      name
    } else {
      parent.get.getPath + " > " + name
    }
  }
  
  def getAncestors: mutable.ArraySeq[Int] = {
    if (level <= 1) {
      mutable.ArraySeq.empty[Int]
    }
    parent.get.getAncestors :+ id
  }
  
  def incrementCount(count: Long): Unit = {
    this.count += count
  }
  
  def getCount: Long = {
    count
  }
  
  private def getLevel: Int = {
    if (parent.isEmpty) {
      0
    } else {
      parent.get.getLevel + 1
    }
  }
}
