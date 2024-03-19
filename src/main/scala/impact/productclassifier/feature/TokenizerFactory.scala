package impact.productclassifier.feature

import org.apache.spark.ml.feature.RegexTokenizer

object TokenizerFactory {

  def tokenizer(featureName: String): RegexTokenizer = {
    new RegexTokenizer()
      .setInputCol(featureName).setOutputCol(featureName + "Tokens")
      .setGaps(false).setPattern("[a-zA-Z0-9$%@\u0080¢£¥°º½¾`.\\-*×±µ\"',/]{3,30}")
      .setToLowercase(false)
  }
}
