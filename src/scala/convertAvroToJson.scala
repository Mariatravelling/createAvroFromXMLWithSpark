/**
  * Created by zehui on 31.12.15.
  */
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import com.databricks.spark.avro._

object convertAvroToJson extends App{

  val cof=new SparkConf().setAppName("readingAvro")
  cof.setMaster("local[*]")
  val sc=new SparkContext(cof)
  val sqlContext=new SQLContext(sc)
  val df=sqlContext.read.avro{
    "Article.avro"
  }
  val newdata=df.withColumn("docID",df("url").substr(48, 58))

  df.registerTempTable("avro")
  df.printSchema()
  //newdata.select("docID").foreach(println)
  val result=sqlContext.sql("SELECT * FROM avro")
  result.repartition(1).write.json("json2.json")
  //df.foreach(println)
  //df.select("allEntities").foreach(println)
  //df.select("url").foreach(println)
}