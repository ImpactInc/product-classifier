package impact.productclassifier.misc

import scala.collection.mutable

class CategoryMap {
  
  final val idsByName: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()
  final val levelOne: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()

  def add(id: Int, categories: Seq[String]): Unit = {
    idsByName.put(categories.mkString(" > ").toLowerCase, id)
    if (categories.size == 1) levelOne.put(categories.head.toLowerCase, id)
  }

  def getId(category: String): Option[Int] = idsByName.get(category)
}