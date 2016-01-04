
import java.io.File
import java.util

import avroschema._
import org.apache.avro.file.DataFileWriter
import org.apache.avro.io.DatumWriter
import org.apache.avro.specific.SpecificDatumWriter

import org.apache.pig.data.{BagFactory, TupleFactory}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import parquet.hadoop.ParquetOutputFormat
import utils.{SentencesLenConstraint, ProcessXML}

import scala.xml.XML
/**
  * Created by wangzehui on 10/27/15.
  */
object readingXML extends App{
  val conf=new SparkConf().setAppName("readingXML")
  conf.setMaster("local[*]")
  val sc=new SparkContext(conf)
  val sqlContext=new SQLContext(sc)
  val file="0000205.xml"
  val parseSentence=new SentencesLenConstraint
  val doc=sc.textFile(file)
  val xml = XML.loadFile(file)

  val mTupleFactory = TupleFactory.getInstance()
  val mBagFactory = BagFactory.getInstance()
  var outerBag = mBagFactory.newDefaultBag()
  val tuple=mTupleFactory.newTuple(xml)
  tuple.append(1)

  val sentenceInfo=parseSentence.exec(tuple)
  val readXml=new ProcessXML()
  val output=readXml.exec(tuple)

  var Abstract=output.get(0).toString
  Abstract=checkNUll(Abstract)
  println("Abstract:"+Abstract)
  val leadParagraph=output.get(1).toString
  val url=output.get(2).toString

  val tit=Title.newBuilder().setTitle(output.get(3).toString).build()
  var titles=new java.util.ArrayList[Title]()
  titles.add(tit)

  val onTitles=Title.newBuilder().setTitle(output.get(4) .toString).build()
  var onlineTitles=new java.util.ArrayList[Title]()
  onlineTitles.add(onTitles)

  val headline=output.get(6).toString
  val content=output.get(7).toString
  println("6"+output.get(5))
  val attributes=output.get(5).toString.split(",")
  var featurePage=checkNUll(attributes(0))
  println("featurePage:"+featurePage)
  var year=checkNUll(attributes(1))
  println("year:"+year)
  var printPageNumber=checkNUll(attributes(2))
  println("printPageNumber:"+printPageNumber)
  var seriesName=checkNUll(attributes(3))
  println("seriesName:"+seriesName)
  var alternateURL=checkNUll(attributes(4))
  println("alternateURL:"+alternateURL)
  var banner=checkNUll(attributes(5))
  println("banner:"+banner)
  var section=checkNUll(attributes(6))
  println("section:"+section)
  var correctionDate=checkNUll(attributes(7))
  println("correctionDate:"+correctionDate)
  var dayOfWeek=checkNUll(attributes(8))
  println("dayOfWeek:"+dayOfWeek)
  var onlineSection=checkNUll(attributes(9))
  println("onlineSection:"+onlineSection)
  var month=checkNUll(attributes(10))
  println("month:"+month)
  var columnNumber=checkNUll(attributes(11))
  println("columnNumber:"+columnNumber)
  var dayOfMonth=checkNUll(attributes(12))
  println("dayOfMonth:"+dayOfMonth)
  var dsk=checkNUll(attributes(13))
  println("dsk:"+dsk)
  var slug=checkNUll(attributes(14))
  println("slug:"+slug)
  var columnName=checkNUll(attributes(15)).substring(0,checkNUll(attributes(15)).length-1)
  println("columnName:"+columnName)

  def checkNUll(parameter: String):String=
  {

    if(parameter.split("=").length==1)
      return ""
    else
      return parameter.split("=")(1)
  }

  def gettingSpan(span: String): String=
  {
    val offset: Array[String] = span.split(",")
    val start_index: String = offset(0).substring(1)
    val end_index: String = offset(1).substring(0, offset(1).length - 1)
    val parsingSpan: String = start_index + ";;" + end_index
    return parsingSpan
  }

  def gettingtokens(tokens: String): util.ArrayList[Token] = {
    var parsingToken = new java.util.ArrayList[Token]()
    val token: Array[String] = tokens.substring(2, tokens.length - 2).split("\\),\\(")
    var i: Int = 0
    while (i < token.length) {

      var ner:String=""
      var start_index: String=""
      var end_index:String=""

      val attributeOfTokens: Array[String] = token(i).split(",")
      val pos: String = attributeOfTokens(0)
      if(pos=="")
        {
          ner= attributeOfTokens(2)
          start_index=attributeOfTokens(3).substring(1)
          end_index= attributeOfTokens(4).substring(0, attributeOfTokens(3).length - 1)
        }
      else {
        ner = attributeOfTokens(1)
        start_index = attributeOfTokens(2).substring(1)
        end_index = attributeOfTokens(3).substring(0, attributeOfTokens(3).length - 1)
      }
      println("start_index:"+start_index)
      println("end_index:"+end_index)
      val tspan=Span.newBuilder().setStartIndex(start_index.toInt).setEndIndex(end_index.toInt).build()
      val tokenss=Token.newBuilder().setNer(ner).setPos(pos).setTSpan(tspan).build()
      println("tokenss.getNer:"+tokenss.getNer)
      parsingToken.add(i,tokenss)
      i=i+1
    }
    return parsingToken
  }

  def gettingTitles(titles: String): util.ArrayList[Title] = {
    var parsingTitles = new java.util.ArrayList[Title]()

    return parsingTitles
  }
  var sentences = new java.util.ArrayList[Sentence]()
  val iter = sentenceInfo.iterator()
  var i=0
  while (iter.hasNext)
    {
      val str=iter.next().getAll
      val id=str.get(0).toString.toInt
      val spans=gettingSpan(str.get(1).toString()).split(";;")
      val span=Span.newBuilder().setEndIndex(spans(1).toInt).setStartIndex(spans(0).toInt).build()
      println(str.get(2).toString)
      val tok=gettingtokens(str.get(2).toString)
      println("getStartIndex:"+span.getStartIndex)
      val sen=Sentence.newBuilder().setDp(str.get(3).toString).setSg(str.get(4).toString()).setSId(id).setSpan(span).setTokens(tok).build()
      println("sen.getDp:"+sen.getDp)
      sentences.add(i,sen)
      i=i+1
    }

  val article=Article.newBuilder()
    .setAbstract$(Abstract)
    .setHeadline(headline)
    .setLeadParagraph(leadParagraph)
    .setAlternateURL(alternateURL)
    .setBanner(banner)
    .setColumnName(columnName)
    .setColumnNumber(columnNumber)
    .setContent(content)
    .setCorrectionDate(correctionDate)
    .setDayOfMonth(dayOfMonth)
    .setDayOfWeek(dayOfWeek)
    .setDsk(dsk)
    .setFeaturePage(featurePage)
    .setMonth(month)
    .setOnlineSection(onlineSection)
    .setOnlineTitles(onlineTitles)
    .setPrintPageNumber(printPageNumber)
    .setPrintSection(section)
    .setSentences(sentences)
    .setSeriesName(seriesName)
    .setSlug(slug)
    .setTitles(titles)
    .setUrl(url)
    .setYear(year).build()
  println(article.getPrintSection)
  val avroSchema=Article.SCHEMA$

  val userDatumWriter: DatumWriter[Article]  = new SpecificDatumWriter[Article]
  val dataFileWriter: DataFileWriter[Article]  = new DataFileWriter[Article](userDatumWriter)

  dataFileWriter.create(Article.getClassSchema, new File("Article.avro"))
  dataFileWriter.append(article)
  dataFileWriter.close()

}
