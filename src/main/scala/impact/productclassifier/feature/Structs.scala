package impact.productclassifier.feature

import org.apache.spark.sql.types.{ArrayType, BooleanType, DecimalType, DoubleType, StringType, StructField, StructType}

import scala.::

object Structs {

  val physicalAttributesStruct: StructType = StructType(
    StructField("color", ArrayType(StringType), nullable = true) ::
    StructField("material", StringType, nullable = true) ::
//    StructField("pattern", StringType, nullable = true) ::
//    StructField("apparelSize", StringType, nullable = true) ::
    StructField("apparelSizeSystem", StringType, nullable = true) ::
    StructField("apparelSizeType", StringType, nullable = true) :: Nil
//    StructField("size", StringType, nullable = true) :: Nil
//    StructField("sizeUnit", StringType, nullable = true) ::
//    StructField("productWeight", DoubleType, nullable = true) ::
//    StructField("weightUnit", StringType, nullable = true) ::
//    StructField("productHeight", DoubleType, nullable = true) ::
//    StructField("productWidth", DoubleType, nullable = true) ::
//    StructField("productLength", DoubleType, nullable = true) ::
//    StructField("lengthUnit", StringType, nullable = true) ::
//    StructField("bundle", BooleanType, nullable = true) ::
//    StructField("condition", StringType, nullable = true) ::
//    StructField("energyEfficiencyClass", StringType, nullable = true) :: Nil
  )

  val targetDemographicsStruct: StructType = StructType(
    StructField("gender", StringType, nullable = true) ::
//    StructField("ageRange", StringType, nullable = true) ::
    StructField("ageGroup", StringType, nullable = true) :: Nil
//    StructField("adult", BooleanType, nullable = true) :: Nil
  )

  val pricingStruct: StructType = StructType(
    StructField("dollarPrice", DecimalType(30, 15), nullable = true) :: 
    StructField("unitOfMeasure", StringType, nullable = true) :: Nil
  )
  
  val taxonomyStruct: StructType = StructType(
    StructField("filterCategory", StringType, nullable = true) :: Nil
  )

  val attributesStruct: StructType = StructType(
    StructField("physicalAttributes", physicalAttributesStruct, nullable = true) ::
    StructField("targetDemographics", targetDemographicsStruct, nullable = true) ::
    StructField("pricing", pricingStruct, nullable = true) ::
    StructField("taxonomy", taxonomyStruct, nullable = true) :: Nil
  )
}
