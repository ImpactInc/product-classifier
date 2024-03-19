
//libraryDependencies ++= Seq(
//  "org.scalameta" %% "munit" % "0.7.26" % Test
////  excludes(("org.apache.spark" %% "spark-core" % "3.2.0").cross(CrossVersion.for3Use2_13)),
////  excludes(("org.apache.spark" %% "spark-sql" % "3.2.0").cross(CrossVersion.for3Use2_13))
//)
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.2" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.3.2" % "provided"
//libraryDependencies += "com.johnsnowlabs.nlp" %% "spark-nlp" % "4.3.0"
libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.12.0"
libraryDependencies += "org.apache.commons" % "commons-csv" % "1.10.0"
libraryDependencies += "com.google.cloud" % "google-cloud-spanner" % "6.37.0"
libraryDependencies += "org.projectlombok" % "lombok" % "1.18.26"
//libraryDependencies += "org.jpmml" % "pmml-sparkml" % "2.1.0"
libraryDependencies += "com.google.cloud" % "google-cloud-storage" % "2.20.2"
//dependencyOverrides += "org.scala-lang.modules" %% "scala-collection-compat" % "2.2.0"
//dependencyOverrides += "com.google.protobuf" %% "protobuf-java" % "2.5.0"
libraryDependencies ++= Seq(
  "ml.dmlc" %% "xgboost4j" % "1.7.4",
  "ml.dmlc" %% "xgboost4j-spark" % "1.7.4"
)

lazy val commonSettings = Seq(
  version := "0.1-SNAPSHOT",
  scalaVersion := "2.12.15"
)

lazy val app = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "product-classifier"
  )

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
