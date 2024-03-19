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
  
  
  def loadWord2VecModel(spec: Word2Vec, location: String): Word2VecModel = {
    val params = spec.extractParamMap().toSeq
      .filter(!_.param.name.contains("Col"))
      .filter(!_.param.name.contains("seed"))
      .filter(!_.param.name.contains("numPartitions"))
      .sortBy(_.param.name)
      .map(pair => s"${pair.param.name}=${pair.value}")
      .mkString("_")
    Word2VecModel.load(s"$location/$params/model")
  } 

  def round(num: Double, places: Int): BigDecimal = {
    BigDecimal(num).setScale(places, BigDecimal.RoundingMode.HALF_UP)
  }
}
