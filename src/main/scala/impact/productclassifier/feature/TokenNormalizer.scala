package impact.productclassifier.feature

import org.apache.commons.lang.StringUtils
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.mutable.ArrayBuffer

class TokenNormalizer(val featureName: String) extends Transformer with DefaultParamsWritable {

  private val measurement = "\\A[0-9]+(?:\\.?[0-9]+)?([°º]?[a-zA-Z]{0,9}(?:/[a-zA-Z])?|'|\"[a-zA-Z]{0,9}|%)\\Z".r
  private val measurementRange = "\\A[0-9]+(?:\\.?[0-9]+)?-[0-9]+(?:\\.?[0-9]+)?([°º]?[a-zA-Z]{0,9}(?:/[a-zA-Z])?|'|\"|%)\\Z".r
  private val twoDimensions = "\\A[0-9]+(?:\\.?[0-9]+)?[*xX×][0-9]+(?:\\.?[0-9]+)?([a-zA-Z]{0,9}|'|\")\\Z".r
  private val threeDimensions = "\\A[0-9]+(?:\\.?[0-9]+)?[*xX×][0-9]+(?:\\.?[0-9]+)?[*xX×][0-9]+(?:\\.?[0-9]+)?([a-zA-Z]{0,9}|'|\")\\Z".r
  
  
  private val exclusionRegexes = Array(
    "[a-z]{2,}[0-9]".r,
    "[aeiou]{5,}".r,
    "[bcdfghjklmnpqrstvwxyz]{5,}".r,
    "[0-9]{6,}".r,
    "[0-9]+[a-z]+[0-9]+".r,
    "[a-z]+[0-9]+[a-z]+".r
  )

  override def transform(df: Dataset[_]): DataFrame = {
    val transformUdf = udf(this.createTransformFunc)
    val colName = s"${featureName}Tokens"
    df
      .withColumn("temp", transformUdf(df(colName)))
      .drop(colName)
      .withColumnRenamed("temp", colName)
  }
  
  private def createTransformFunc: Seq[String] => Seq[String] = { tokens =>
    val normalized = splitTokens(tokens)
      .map(_.toLowerCase)
      .filter(s => s != null && s.length >= 2 && applyExclusionRegexes(s))
    
    val finalTokens: ArrayBuffer[String] = new ArrayBuffer[String]()
    for (tok <- normalized) {
      finalTokens.append(tok)
      if (containsDigit(tok)) {
        val measurement = replaceMeasurement(tok)
        if (measurement.contentEquals(tok)) {
          finalTokens.append(measurement)
        }
      }
    } 
    
    // TODO: Make configurable
    finalTokens
  }
  
  private def splitTokens(tokens: Seq[String]): Seq[String] = {
    val split1: ArrayBuffer[String] = new ArrayBuffer()
    for (tok <- tokens) {
      val t = splitAroundPunctuation(replaceDuplicatePunctuation(cleanWordApostrophes(tok), "\""))
      if (t.contains(" ")) {
        split1.appendAll(t.split(' '))
      } else {
        split1.append(t)
      }
    }
    
    val split2: ArrayBuffer[String] = new ArrayBuffer()
    for (tok <- split1) {
      val t = splitCamelCaseWords(tok)
      if (t.contains(" ")) {
        split2.appendAll(t.split(' '))
      } else {
        split2.append(t)
      }
    }
    
    split2.map(cleanEdgePunctuation)
  }
  
  private def cleanWordApostrophes(s: String): String = {
    // TODO: Optimize
    StringUtils.replaceEach(s, 
      Array("'t", "'s", "'d", "'ll", "'re", "'m", "'ve", "''"), 
      Array("t", "", "", "", "", "", "", "\""))
  }

  private def replaceDuplicatePunctuation(s: String, p: String): String = {
    // TODO: Optimize (can do all punctuation simultaneously)
    (p + "{2,}").r.replaceAllIn(s, p)
  }

  private def splitAroundPunctuation(s: String): String = {
    var ss = s
    if (ss.contains("-")) {
      ss = replaceDuplicatePunctuation(ss, "-")
      ss = "([a-zA-Z])-([a-zA-Z])".r.replaceAllIn(ss, "$1 $2")
    }
    if (StringUtils.containsAny(ss, ",.*")) {
      ss = replaceDuplicatePunctuation(ss, ",")
      ss = replaceDuplicatePunctuation(ss, "\\.")
      ss = "(.)[.,*]+([^0-9\\-\\s])".r.replaceAllIn(ss, "$1 $2")
      ss = "([^0-9\\-])[.,]+(.)".r.replaceAllIn(ss, "$1 $2")
    }
    if (ss.contains("/")) {
      ss = "(.)/(..)".r.replaceAllIn(ss, "$1 $2")
    }
    ss
  }
  
  private def cleanEdgePunctuation(s: String): String = {
    var ss = StringUtils.strip(s, ",-*:/")
    ss = StringUtils.stripEnd(ss, ".")
    ss = StringUtils.stripStart(ss, "'\"")
    ss = ss.replaceAll("\\A\\.([^0-9])", "$1")
    ss.replaceAll("([^0-9])['\"]+\\Z", "$1")
  }
  
  private def stripTrailingPunctuation(token: String): String = {
    StringUtils.strip(token, "-*,")
  }
  
  private def splitCamelCaseWords(s: String): String = {
    "([a-z]{2})([A-Z][a-z])".r.replaceAllIn(s, "$1 $2")
  }
  
  private def applyExclusionRegexes(token: String): Boolean = {
    for (regex <- exclusionRegexes) {
      if (regex.findFirstMatchIn(token).nonEmpty) {
        return false
      }
    }
    true
  }
  
  private def cleanCommasFromNumber(token: String): String = {
    var tok = ",(0-9){3}".r.replaceAllIn(token, "$1")
    if (tok.contains(',')) {
      tok = tok.replace(",", "")
    }
    tok
  }
  
  private def containsDigit(token: String): Boolean = {
    StringUtils.containsAny(token, "0123456789")
  }
  
  private def replaceMeasurement(token: String): String = {
    if (!containsDigit(token)) {
      return token
    }
    if (StringUtils.containsAny(token, "x*×")) {
      return replaceDimensions(token)
    }
    var tok = token
    if (token.contains(',')) {
      tok = cleanCommasFromNumber(tok)
    }
    val isRange = StringUtils.contains(tok, "-")
    val regex = if (isRange) measurementRange else measurement
    
    val opt = regex.findFirstMatchIn(tok)
    if (opt.isEmpty) {
      return tok
    }
    
    val sb = new StringBuilder("<")
    sb.append(if (tok.contains(".")) "float" else "int")
    if (isRange) {
      sb.append("_range")
    }
    
    val unit: String = opt.get.group(1)
    if (unit == null) {
      return sb.append(">").toString()
    }
    val s = unit.toLowerCase match {
      case "mg" => "mg"
      case "g" => "g"
      case "kg" => "kg"
      case "kgs" => "kg"
      case "oz" => "oz"
      case "lbs" => "lbs"
      case "ml" => "ml"
      case "l" => "l"
      case "kl" => "kl"
      case "mm" => "mm"
      case "cm" => "cm"
      case "m" => "m"
      case "km" => "km"
      case "\"" => "inch"
      case "quot" => "inch"
      case "in" => "inch"
      case "inch" => "inch"
      case "inches" => "inch"
      case "'" => "ft"
      case "ft" => "ft"
      case "psi" => "psi"
      case "%" => "%"
      case "percent" => "%"
      case "ohms" => "ohm"
      case "ohm" => "ohm"
      case "w" => "watt"
      case "watts" => "watt"
      case "wattage" => "watt"
      case "wh" => "watt_hour"
      case "ah" => "amp_hour"
      case "mah" => "milli_amp_hour"
      case "v" => "volt"
      case "volts" => "volt"
      case "voltage" => "volt"
      case "min" => "minute"
      case "mins" => "minute"
      case "h" => "hour"
      case "hr" => "hour"
      case "hrs" => "hour"
      case "cc" => "cc"
      case _ => cleanUnit(unit.toLowerCase)
    }
    sb.append("_").append(s).append(">").toString()
  }
  
  private def replaceDimensions(token: String): String = {
    def isThreeDimensional(token: String): Boolean = {
      StringUtils.countMatches(token, "x") == 2 || StringUtils.countMatches(token, "*") == 2 || 
        StringUtils.countMatches(token, "×") == 2
    }
    val dimensions = if (isThreeDimensional(token)) 3 else 2
    val opt = if (dimensions == 3) threeDimensions.findFirstMatchIn(token) else twoDimensions.findFirstMatchIn(token)
    if (opt.isEmpty) {
      return token
    }
    val sb = new StringBuilder(s"<${dimensions}d_")
    sb.append(if (token.contains(".")) "float" else "int")
    val unit: String = opt.get.group(1)
    if (unit == null) {
      return sb.append(">").toString()
    }
    val s = unit.toLowerCase match {
      case "mm" => "mm"
      case "cm" => "cm"
      case "m" => "m"
      case "km" => "km"
      case "\"" => "inch"
      case "quot" => "inch"
      case "in" => "inch"
      case "inch" => "inch"
      case "inches" => "inch"
      case "'" => "ft"
      case "ft" => "ft"
      case _ => cleanUnit(unit.toLowerCase)
    }
    sb.append("_").append(s).append(">").toString()
  }
  
  private def cleanUnit(unit: String): String = {
    if (unit.length > 1) {
      return unit.charAt(0) match {
        case ''' => "ft"
        case '"' => "inch"
        case _ => {
          if (unit.length > 4 && StringUtils.startsWithAny(unit, Array("mm", "cm", "hz"))) {
            unit.substring(0, 2)
          } else {
            if (unit.startsWith("quot")) {
              "inch"
            } else {
              unit
            }
          }
        }
      }
    }
    unit
  }

  def testSplitAroundPunctuation(): Unit = {
    val s1 = "abc.def..ghi.2.0"
    val s2 = "2..0 2,,0 1--2"
    val s3 = "0.1-0.3"
    val s4 = "aa-bb--cc-3-4"
    val s5 = ".1.,2"
    val s6 = "a,,b,3,0"
    println(f"$s1%-20s => ${splitAroundPunctuation(s1)}")
    println(f"$s2%-20s => ${splitAroundPunctuation(s2)}")
    println(f"$s3%-20s => ${splitAroundPunctuation(s3)}")
    println(f"$s4%-20s => ${splitAroundPunctuation(s4)}")
    println(f"$s5%-20s => ${splitAroundPunctuation(s5)}")
    println(f"$s6%-20s => ${splitAroundPunctuation(s6)}")
  }
  
  def testCleanEdgePunctuation(): Unit = {
    val s1 = "*,-abc..*--"
    val s2 = ".556,"
    val s3 = ",556*-"
    val s4 = "-556."
    println(f"$s1%-20s => ${cleanEdgePunctuation(s1)}")
    println(f"$s2%-20s => ${cleanEdgePunctuation(s2)}")
    println(f"$s3%-20s => ${cleanEdgePunctuation(s3)}")
    println(f"$s4%-20s => ${cleanEdgePunctuation(s4)}")
  }

  def testReplaceMeasurements(): Unit = {
    val s1 = "3.42×2.55×2.55in"
    val s2 = ".50×1.18in"
    val s3 = "3.73×2.16×1.7"
    val s4 = "7.00×6.00×6.00cm"
    val s5 = "900-1,100"
    val s6 = "16\"x22\""
    val s7 = "1,100,000"
    val s8 = "25m/s"
    val s9 = "5'4in"
    val s10 = "23.4\"W"
    val s11 = "212ºF"
    val s12 = "29.25\"Length"
    println(f"$s1%-20s => ${replaceMeasurement(s1)}")
    println(f"$s2%-20s => ${replaceMeasurement(s2)}")
    println(f"$s3%-20s => ${replaceMeasurement(s3)}")
    println(f"$s4%-20s => ${replaceMeasurement(s4)}")
    println(f"$s5%-20s => ${replaceMeasurement(s5)}")
    println(f"$s6%-20s => ${replaceMeasurement(s6)}")
    println(f"$s7%-20s => ${replaceMeasurement(s7)}")
    println(f"$s8%-20s => ${replaceMeasurement(s8)}")
    println(f"$s9%-20s => ${replaceMeasurement(s9)}")
    println(f"$s10%-20s => ${replaceMeasurement(s10)}")
    println(f"$s11%-20s => ${replaceMeasurement(s11)}")
    println(f"$s12%-20s => ${replaceMeasurement(s12)}")
  }
  
  def testSplitCamelCaseWords(): Unit = {
    val s1 = "2.54cmIncludes"
    val s2 = "MaterialColorBlackSize45*40cmW"
    val s3 = "SizingSizeSmallMediumLargeX"
    val s4 = "glovesAdjustable"
    val s5 = "bearingwhich"
    val s6 = "UPPER"
    val s7 = "aBcDeeFFggHH"
    val s8 = "displayAMD"
    val s9 = "110PresetNOVision"
    println(f"$s1%-20s => ${splitCamelCaseWords(s1)}")
    println(f"$s2%-20s => ${splitCamelCaseWords(s2)}")
    println(f"$s3%-20s => ${splitCamelCaseWords(s3)}")
    println(f"$s4%-20s => ${splitCamelCaseWords(s4)}")
    println(f"$s5%-20s => ${splitCamelCaseWords(s5)}")
    println(f"$s6%-20s => ${splitCamelCaseWords(s6)}")
    println(f"$s7%-20s => ${splitCamelCaseWords(s7)}")
    println(f"$s8%-20s => ${splitCamelCaseWords(s8)}")
    println(f"$s9%-20s => ${splitCamelCaseWords(s9)}")
  }

  override def transformSchema(schema: StructType): StructType = schema

  override def copy(extra: ParamMap): TokenNormalizer = defaultCopy(extra)

  override val uid: String = Identifiable.randomUID("tokenNormalizer")
}
