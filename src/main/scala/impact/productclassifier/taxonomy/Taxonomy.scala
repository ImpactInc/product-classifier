package impact.productclassifier.taxonomy

import java.io.IOException
import scala.io.{Codec, Source}

class Taxonomy {
  
  val root: Category = new Category("root", -1, None)
  
  def populate(): this.type = {
    val lines = readLinesFromCsv()
    val categories: Seq[(Int, Array[String])] = lines
      .map(_.toLowerCase.split(";"))
      .sortBy(_.length)
      .map(values => (Integer.parseInt(values.head), values.tail)).toIndexedSeq
    
    categories.foreach(elem => addCategory(elem._1, elem._2))
    this
  }
  
  private def addCategory(id: Int, path: Seq[String]): Unit = {
//    if (path.head.equalsIgnoreCase("religious & ceremonial") || path.head.equalsIgnoreCase("mature")) {
//      return
//    }
    var node = root
    for (name <- path.slice(0, path.length - 1)) {
      node = node.getSubCategory(name)
    }
    node.addSubCategory(path.last, id)
  }

  private def readLinesFromCsv(): Array[String] = {
    try {
//      if (isOnDataproc) {
//        Files.list(Paths.get(".")).forEach(println)
//        Using(Source.fromFile("google_taxonomy.csv")(Codec.UTF8)) { source =>
//          source.getLines().toArray
//        }.get
//      } else {
//      }
      Source.fromResource("google_taxonomy.csv")(Codec.UTF8).getLines().toArray.tail
      //      val path = Paths.get(System.getenv("HOME") + "/Documents/PDS/Taxonomy/google_taxonomy.csv")
//      Files.readAllLines(path).asScala.tail
    } catch {
      case _: IOException =>
        Array()
    }
  }
}
