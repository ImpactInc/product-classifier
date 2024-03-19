package impact.productclassifier.feature

import org.apache.spark.ml.feature._

object Util {

  def vectorizer(featureName: String, vocabSize: Int): CountVectorizer = {
    new CountVectorizer().setInputCol(featureName + "Tokens").setOutputCol(featureName + "Vector").setVocabSize(vocabSize)
  }
  
  def stringIndexer(featureName: String): StringIndexer = {
    new StringIndexer().setInputCol(featureName).setOutputCol(featureName + "Index").setStringOrderType("alphabetAsc")
  }
  
  def oneHotEncoder(featureName: String): OneHotEncoder = {
    new OneHotEncoder().setInputCol(featureName + "Index").setOutputCol(featureName + "Vector")
  }

  def round(num: Double, places: Int): BigDecimal = {
    BigDecimal(num).setScale(places, BigDecimal.RoundingMode.HALF_UP)
  }
}
