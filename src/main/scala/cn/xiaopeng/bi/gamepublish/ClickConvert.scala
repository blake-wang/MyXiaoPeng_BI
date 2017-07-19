package cn.xiaopeng.bi.gamepublish

import cn.wanglei.bi.ConfigurationUtil
import cn.xiaopeng.bi.utils.{DateUtils, Hadoop, StringUtils}
import kafka.serializer.StringDecoder
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapred.lib.CombineTextInputFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by JSJSB-0071 on 2017/7/13.
 */
object ClickConvert {
  //2017-05-08 17:38:01,057 [INFO] bi: bi_channel|3|eaaqy_tjzt_3151208029|100.97.15.51|2017-05-08 17:38:01|3208||
  def parseChannelToToTmpTable(rdd:RDD[String],sqlContext:SQLContext)={
    val channelRdd = rdd.filter(line => line.contains("bi_channel")).map(line =>{
      val splited = line.split("\\|",-1)
      try {
        Row(StringUtils.defaultEmptyTo21(splited(2)), Integer.parseInt(splited(5)), splited(4).split(" ")(0), Integer.parseInt(splited(1)), splited(3))
      } catch {
        case ex:Exception =>{
          Row("pyw",0,"0000-00-00 00",0,"")
        }
      }

    })

    val channelStruct = (new StructType)
      .add("expand_channel", StringType)
      .add("game_id",IntegerType)
      .add("publish_time",StringType)
      .add("type", IntegerType)
      .add("ip", StringType)
    val channelDF = sqlContext.createDataFrame(channelRdd,channelStruct);
    channelDF.registerTempTable("channel")
    ClickConvert
  }
  /**
    * parse request log detail
    * @param rdd
    * @param sqlContext
    * @return
    */
  def parseRequestToTmpTable(rdd:RDD[String],sqlContext:SQLContext)={
    val requestRdd = rdd.filter(line => !line.contains("HTTP/1.0\" 404")).filter(line=>StringUtils.isRequestLog(line,".*0800] \"GET /Ssxy/loadComplete[?]p=[\\d|_|a-z|A-Z]+&g=[\\d]+.*"))
      .map(line=>{
        try {
          val ip = line.split("\"", -1)(6).split(",")(0)
          val sp = line.split(" ")(6).split("p=", -1)(1).split("\"\"", -1)(0)
          val sp1 = sp.split("&g=", -1)
          val requestDate = DateUtils.getDateForRequest(line)
          if (sp1(1) == "") {
            Row("pyw", 0, requestDate)
          } else {
            Row(StringUtils.defaultEmptyTo21(sp1(0)), Integer.parseInt(sp1(1).split("&", -1)(0)), requestDate, 4, ip)
          }
        } catch {
          case  ex:Exception=>{
            Row("pyw",0,"0000-00-00",0,"")
          }
        }
      })
    val requestStruct = (new StructType()).add("expand_channel",StringType)
      .add("game_id",IntegerType).add("publish_time",StringType).add("type",IntegerType).add("ip", StringType)
    val requestDF = sqlContext.createDataFrame(requestRdd,requestStruct)
    requestDF.registerTempTable("request")
    ClickConvert

  }
  /**
    * 把 全量+实时日志 深度联运转化-临时表
    * @param rdd
    * @param sqlContext
    */

  def convertPubGameLogsToDfTmpTable(rdd:RDD[String],sqlContext:SQLContext) = {
    //深度联运日志 ，Struct，DF,temp_table
    val pubGameRdd = rdd.filter(line =>line.contains("bi_pubgame")).map(line=>{
      try {
        val splited = line.split("\\|", -1)
        Row(Integer.parseInt(splited(1)))
      } catch {
        case  ex:Exception=>{
          Row(-1)
        }
      }
    })

    val pubGameStruct = (new StructType).add("game_id",IntegerType)
    val pubGameDf = sqlContext.createDataFrame(pubGameRdd,pubGameStruct);
    pubGameDf.registerTempTable("pubgame")
    val lastPubGame = sqlContext.sql("select distinct t.* from (select * from pubgame union select * from pubgame_full) t")
    lastPubGame.registerTempTable("lastPubGame")
    ClickConvert
  }

  /**
    * 获取全量深度联运数据
    * @param sc
    * @param sqlContext
    */

//  def getPubGameFullData(sc:SparkContext,sqlContext:SQLContext)={
//    val regiRdd = sc.newAPIHadoopFile(ConfigurationUtil.getProperty("gamepublish.offline.pubgame"),
//      classOf[CombineTextInputFormat],
//      classOf[LongWritable],
//      classOf[Text])
//
//
//  }

}
