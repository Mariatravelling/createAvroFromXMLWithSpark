
import com.databricks.spark.avro.AvroDataFrameWriter
import org.apache.pig.data.{BagFactory, TupleFactory}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import utils.{ProcessXML, SentencesLenConstraint}

import scala.collection.mutable.ListBuffer
import scala.xml.XML

/**
  * Created by wangzehui on 1/3/16.
  */
object createAvroFromXML extends App {
  val conf=new SparkConf().setAppName("createAvroFromXML")
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
  var Abstract: String=output.get(0).toString
  Abstract=checkNUll(Abstract)
  val leadParagraph: String=output.get(1).toString
  val url: String=output.get(2).toString

  var tit: String=output.get(3).toString
  var titles: ListBuffer[Title]=gettingTitles(tit)
  println("titles"+titles)
  var ontit: String=output.get(4).toString
  var onlineTitles: ListBuffer[Title]=gettingTitles(ontit)
  println("onlinetitles"+onlineTitles)

  val headline: String=output.get(6).toString
  val content: String=output.get(7).toString
  val attributes=output.get(5).toString.split(",")
  var featurePage: String=checkNUll(attributes(0))
  var year: String=checkNUll(attributes(1))
  var printPageNumber: String=checkNUll(attributes(2))
  var seriesName: String=checkNUll(attributes(3))
  var alternateURL: String=checkNUll(attributes(4))
  var banner: String=checkNUll(attributes(5))
  var section: String=checkNUll(attributes(6))
  var correctionDate: String=checkNUll(attributes(7))
  var dayOfWeek: String=checkNUll(attributes(8))
  var onlineSection: String=checkNUll(attributes(9))
  var month: String=checkNUll(attributes(10))
  var columnNumber: String=checkNUll(attributes(11))
  var dayOfMonth: String=checkNUll(attributes(12))
  var dsk: String=checkNUll(attributes(13))
  var slug: String=checkNUll(attributes(14))
  var columnName: String=checkNUll(attributes(15)).substring(0,checkNUll(attributes(15)).toString.length-1)

  val iter = sentenceInfo.iterator()
  var sentence = new ListBuffer[Sentences]()
  while (iter.hasNext)
  {
    val str=iter.next().getAll
    val id=str.get(0).toString.toInt
    val spans=gettingSpan(str.get(1).toString())
    val tok=gettingtokens(str.get(2).toString)
    val se=Sentences.apply(id,spans,tok,str.get(3).toString,str.get(4).toString())
    sentence += se
  }
  case class Token(
                    pos: String,
                    ner: String,
                    t_span:Span
                  )
  case class Span(
                   start_index: Int,
                   end_index:  Int
                 )
  case class Sentences(
                        s_id: Int,
                        span: Span,
                        tokens: ListBuffer[Token],
                        dp: String,
                        sg: String
                      )
  case class Title(
                    title:String
                  )

  def checkNUll(parameter: String):String=
  {

    val parameter1=parameter.toString
    if(parameter1.split("=").length==1)
      return ""
    else
      return parameter1.split("=")(1)
  }

  def gettingTitles(titles: String): ListBuffer[Title] = {
    var parsingTitles = new ListBuffer[Title]()
    val b=Title.apply(titles)
    parsingTitles+=b
    return parsingTitles
  }

  def gettingSpan(span: String): Span=
  {
    val offset: Array[String] = span.split(",")
    val start_index: String = offset(0).substring(1)
    val end_index: String = offset(1).substring(0, offset(1).length - 1)
    val parsingSpan: String = start_index + ";;" + end_index
    val sp=Span.apply(start_index.toInt,end_index.toInt)
    return sp
  }

  def gettingtokens(tokens: String): ListBuffer[Token] = {
    var parsingToken = new ListBuffer[Token]()
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
      val s=Span.apply(start_index.toInt,end_index.toInt)
      val a=Token.apply(ner,pos,s)
      parsingToken += a
      i=i+1
    }
    return parsingToken
  }

  val result=sc.makeRDD(Seq((Abstract,leadParagraph,url,titles,dsk,onlineSection,printPageNumber,section,slug,
    columnNumber,banner,correctionDate,featurePage,columnName,seriesName,dayOfMonth,month,year,dayOfWeek,headline,content,sentence)))
  val out=sqlContext.createDataFrame(result).toDF("abstract","leadParagraph","url","titles","dsk","onlineSection",
    "printPageNumber","printSection","slug","columnNumber","banner","correctionDate","featurePage","columnName","seriesName","dayOfMonth","month","year","dayOfWeek","headline","content","sentences")
  // println(out.foreach(println))
  out.repartition(1).write.json("test6.json")
  out.repartition(1).write.avro("test6.avro")

}