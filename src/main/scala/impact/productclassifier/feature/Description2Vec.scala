package impact.productclassifier.feature

import impact.productclassifier.DataLoader
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.sql.functions.{col, size}
import org.apache.spark.storage.StorageLevel
import impact.productclassifier.App.spark

object Description2Vec {

  import spark.implicits._

//  private val descTokenizer: RegexTokenizer = new RegexTokenizer()
//    .setInputCol("sentence").setOutputCol("tokens")
//    .setGaps(false).setPattern("\\w+")

  private val desc2Vec: Word2Vec = new Word2Vec()
    .setInputCol("tokens").setOutputCol("descVec")
    .setVectorSize(200)
    .setWindowSize(5)
    .setMinCount(100)
    .setMaxIter(50)
    .setNumPartitions(128)
  
  def fit(): Word2VecModel = {
    val df = DataLoader.readAllData($"name")
      .limit(10000000)
//      .select(explode(sentences(lower($"name"))).as("tokens"))
      .persist(StorageLevel.DISK_ONLY)
    val pipeline = new Pipeline().setStages(Array(
      desc2Vec
    ))
    println(s"Num partitions: ${df.rdd.getNumPartitions}")
    println("Num sentences: " + df.count())
    val trainingData = df
      .filter(size($"tokens").between(4, 50))
      .repartition(128)
      .persist(StorageLevel.DISK_ONLY)
//    val df3 = trainingData.filter(size(col("tokens")).geq(20))
    println("Num short sentences: " + df.filter(size(col("tokens")).leq(3)).count())
    println("Num long sentences: " + df.filter(size(col("tokens")).geq(20)).count())
    println("Num very long sentences: " + df.filter(size(col("tokens")).geq(50)).count())
    println("Num sentences: " + trainingData.count())
    
    val pipelineModel = pipeline.fit(trainingData)

    val model = pipelineModel.stages.last.asInstanceOf[Word2VecModel]
    println(s"Vocab size: ${model.getVectors.count()}")
    showSynonyms(model, "best")
    showSynonyms(model, "luxury")
    showSynonyms(model, "black")
    showSynonyms(model, "color")
    showSynonyms(model, "phone")
    showSynonyms(model, "apple")
    showSynonyms(model, "samsung")
    showSynonyms(model, "food")
    showSynonyms(model, "shirt")
    showSynonyms(model, "shoes")
    showSynonyms(model, "necklace")
    showSynonyms(model, "tire")
    showSynonyms(model, "car")
    showSynonyms(model, "laptop")
    showSynonyms(model, "milk")
    showSynonyms(model, "chair")
    showSynonyms(model, "mattress")
    showSynonyms(model, "hammer")
    showSynonyms(model, "bra")
    showSynonyms(model, "makeup")
    showSynonyms(model, "book")
    showSynonyms(model, "novel")
    showSynonyms(model, "pen")
    showSynonyms(model, "office")
    showSynonyms(model, "stapler")
    showSynonyms(model, "football")
    showSynonyms(model, "tennis")
    showSynonyms(model, "golf")
    showSynonyms(model, "game")
    showSynonyms(model, "toy")
    showSynonyms(model, "doll")
    showSynonyms(model, "vehicle")
    
    df.unpersist()
    trainingData.unpersist()
    model
  }
  
  private def showSynonyms(model: Word2VecModel, word: String): Unit = {
    try {
      val results = model.findSynonymsArray(word, 10).map(elem => (elem._1, round(elem._2, 2)))
      val synonyms = results.map(elem => f"${elem._1}%-20s").mkString("", "", "")
      val scores = results.map(elem => f"${elem._2}%-20s").mkString("", "", "")
      println(f"$word%-20s%n$synonyms%n$scores")
      println("-" * (9 * 20))
    } catch {
      case _: Exception => 
        println(word + ": N/A")
        println("-" * (9 * 20))
    }
  }

  private def round(num: Double, places: Int): BigDecimal = {
    BigDecimal(num).setScale(places, BigDecimal.RoundingMode.HALF_UP)
  }
}
