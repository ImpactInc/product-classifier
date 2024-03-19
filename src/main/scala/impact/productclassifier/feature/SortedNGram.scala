package impact.productclassifier.feature

import org.apache.spark.ml.feature.NGram

class SortedNGram extends NGram {
  override protected def createTransformFunc: Seq[String] => Seq[String] = {
    _.iterator.sliding($(n)).withPartial(false).map(_.sorted.mkString(" ")).toSeq
  }
}
