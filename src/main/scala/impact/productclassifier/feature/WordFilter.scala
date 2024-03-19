package impact.productclassifier.feature

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{Column, DataFrame, Dataset, functions}
import org.apache.spark.sql.functions.{col, regexp_extract}
import org.apache.spark.sql.types.StructType

class WordFilter(val featureName: String) extends Transformer {

  private val regex1 = "([aeiou]{5,})"
  private val regex2 = "([bcdfghjklmnpqrstvwxyz]{4,})"
  private val regex3 = "(\\A[bcdfghjklmnpqrstvwxz]{2,3}\\z)"
  private val regex4 = "([0-9]{3,})"
  private val regex5 = "([0-9]+[a-z]+[0-9]+)"
  private val regex6 = "([a-z]+[0-9]+[a-z]+)"

  private val regexFilter = (w: Column) => applyRegex(w, regex1) && applyRegex(w, regex2) && applyRegex(w, regex3) &&
    applyRegex(w, regex4) && applyRegex(w, regex5) && applyRegex(w, regex6)

  private def applyRegex(column: Column, regex: String): Column = {
    functions.length(regexp_extract(column, regex, 1)).leq(0)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val colName = featureName + "Tokens"
    dataset.withColumn("temp", functions.filter(col(colName), regexFilter))
      .drop(colName).withColumnRenamed("temp", featureName + "Tokens")
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = schema

  override val uid: String = Identifiable.randomUID("wordfilter")
}
