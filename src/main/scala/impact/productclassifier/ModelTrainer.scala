package impact.productclassifier

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.functions.{col, concat_ws}
import org.apache.spark.storage.StorageLevel
import App.spark
import impact.productclassifier.feature.{TokenNormalizer, TokenizerFactory}

object ModelTrainer {

  import spark.implicits._

  def createName2VecModel(): Unit = {
    val tokenizer = TokenizerFactory.tokenizer("name")
    val tokenNormalizer = new TokenNormalizer("name") 
    val name2Vec: Word2Vec = new Word2Vec()
      .setInputCol("nameTokens").setOutputCol("nameVector")
      .setVectorSize(300)
      .setWindowSize(5)
      .setMinCount(200)
      .setMaxIter(1)
      .setNumPartitions(128)
    
    val df = DataLoader.readAllData(concat_ws(" ", $"name", $"manufacturer").as("name")).repartition(256)
    val features = tokenNormalizer.transform(tokenizer.transform(df)).persist(StorageLevel.MEMORY_AND_DISK_SER)
    println(s"Features partitions: ${features.rdd.getNumPartitions}")
    val name2VecModel = name2Vec.fit(features)
    println(s"Num records: ${features.count()}")
    val params = name2Vec.extractParamMap().toSeq
      .filter(!_.param.name.contains("Col"))
      .filter(!_.param.name.contains("seed"))
      .filter(!_.param.name.contains("numPartitions"))
      .sortBy(_.param.name)
      .map(pair => s"${pair.param.name}=${pair.value}")
      .mkString("_") + "_manufacturer"
    println("name2vec")
    println(params)
    name2VecModel.write.overwrite().save(s"gs://product-classifier/model/name2vec/$params/model")
    println(s"Vocab size: ${name2VecModel.getVectors.count()}")
  }

  def createDesc2VecModel(): Unit = {
    val tokenizer = TokenizerFactory.tokenizer("description")
    val tokenNormalizer = new TokenNormalizer("description")
    val desc2Vec: Word2Vec = new Word2Vec()
      .setInputCol("descriptionTokens").setOutputCol("descriptionVector")
      .setVectorSize(300)
      .setWindowSize(5)
      .setMinCount(700)
      .setMaxIter(1)
      .setNumPartitions(512)

    val catCols = (1 to 7).map(i => col(s"cat$i"))
    val train = DataLoader.readAllData(Array($"description") ++ catCols: _*).repartition(512)
//    val (train, _) = DataLoader.sampleAndSplitDataSet(df, 200000, 7, 1.0)
    val features = tokenNormalizer.transform(tokenizer.transform(train)).persist(StorageLevel.DISK_ONLY)
    println(s"Features partitions: ${features.rdd.getNumPartitions}")
    val desc2VecModel = desc2Vec.fit(features)
    println(s"Num records: ${features.count()}")
    val params = desc2Vec.extractParamMap().toSeq
      .filter(!_.param.name.contains("Col"))
      .filter(!_.param.name.contains("seed"))
      .filter(!_.param.name.contains("numPartitions"))
      .sortBy(_.param.name)
      .map(pair => s"${pair.param.name}=${pair.value}")
      .mkString("_")
    println("desc2vec")
    println(params)
    desc2VecModel.write.overwrite().save(s"gs://product-classifier/model/desc2vec/$params/model")
    println(s"Vocab size: ${desc2VecModel.getVectors.count()}")
  }
}
