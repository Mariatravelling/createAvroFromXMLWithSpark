name := "studentjob"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.4.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.0" excludeAll ExclusionRule(organization = "javax.servlet")

libraryDependencies += "net.liftweb" % "lift-json_2.10" % "2.6"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.4.0"

libraryDependencies += "com.databricks" % "spark-avro_2.10" % "2.0.1"

libraryDependencies += "org.apache.avro" % "avro-mapred" % "1.7.7" classifier "hadoop2"

libraryDependencies += "org.apache.pig" % "pig" % "0.12.0"

libraryDependencies += "org.apache.pig" % "piggybank" % "0.12.0"

libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.4.1"

libraryDependencies += "edu.stanford.nlp" % "stanford-parser" % "3.4.1"

libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.4.1" classifier "models"

libraryDependencies += "org.spark-project.hive" % "hive-exec" % "1.2.1.spark"

libraryDependencies += "org.spark-project" % "spark-core_${scala.version}" % "0.7.3"

libraryDependencies += "org.apache.avro" % "avro" % "${avro.version}"

libraryDependencies += "org.apache.parquet" % "parquet-avro" % "1.7.0"
