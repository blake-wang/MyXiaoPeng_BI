package cn.xiaopeng.bi.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}

/**
  * Created by bigdata on 17-8-1.
  */
object StreamingUtils {

  /**
    * 把 全量+实时日志  深度联运转化 成 临时表
    */
  def convertPubGameLogsToDfTmpTable(rdd: RDD[String], hiveContext: HiveContext) {
    val pubGameRdd = rdd.filter(line => line.contains("bi_pubgame")).map(line => {
      val splited = line.split("\\|", -1)
      try {
        Row(Integer.parseInt(splited(1)))
      } catch {
        case e: Exception => {
          Row(-1)
        }
      }
    })

    val pubGameStruct = new StructType().add("game_id",IntegerType)
    val pubGameDF = hiveContext.createDataFrame(pubGameRdd,pubGameStruct)
    pubGameDF.registerTempTable("pubgame")
    hiveContext.sql("use yyft")
    val lastPubGame = hiveContext.sql("select distinct t.* from (select * from pubgame union select distinct game_id from ods_publish_game) t")
    lastPubGame.cache()
    lastPubGame.registerTempTable("lastPubGame")
  }

  def parseChannelToTmpTable(rdd:RDD[String],sqlContext: SQLContext)={
    //2017-05-08 17:38:01,057 [INFO] bi: bi_channel|3|eaaqy_tjzt_3151208029|100.97.15.51|2017-05-08 17:38:01|3208||
    val channelRdd = rdd.filter(line=>{
        line.contains("bi_channel")
    }).map(line=>{
        try {
          val splited = line.split("\\|", -1)
          Row(StringUtils.defaultEmptyTo21(splited(2)), Integer.parseInt(splited(5)), splited(4).split(" ")(0), Integer.parseInt(splited(1)), splited(3))
        } catch {
          case ex:Exception =>{
            Row("pyw",0, "0000-00-00 00", 0, "")
          }
        }
    })
    val channelStruct = new StructType()
      .add("expand_channel",StringType)
      .add("game_id",IntegerType)
      .add("publish_time",StringType)
      .add("type",IntegerType)
      .add("ip",StringType)
    val channelDF = sqlContext.createDataFrame(channelRdd,channelStruct)
    channelDF.registerTempTable("channel")
    StreamingUtils
  }

  def parseRequestToTmpTable(rdd:RDD[String], sqlContext: SQLContext)={
//      val requestRdd = rdd.filter(line=> !line.contains("HTTP/1.0\" 404")).filter()
    val requestRdd = rdd.filter(line=>line.contains("HTTP/1.0\" 404")).filter(line => StringUtils.isRequestLog(line, ".*0800] \"GET /Ssxy/loadComplete[?]p=[\\d|_|a-z|A-Z]+&g=[\\d]+.*"))
      .map(line=>{
        try {
          val ip = line.split(" ", -1)(6).split(",")(0)
          val sp = line.split(" ")(6).split("p=", -1)(1).split("\"\"", -1)(0)
          val sp1 = sp.split("&g=", -1)
          val requestDate = DateUtils.getDateForRequest(line)
          if (sp1(1).equals("")) {
            Row("pyw", 0, requestDate)
          } else {
            Row(StringUtils.defaultEmptyTo21(sp1(0)), Integer.parseInt(sp1(1).split("&", -1)(0)), requestDate, 4, ip)
          }
        } catch {
          case w:Exception =>{
            Row("pyw", 0, "0000-00-00", 0, "")
          }
        }
      })
    val requestStruct = (new StructType).add("expand_channel", StringType)
      .add("game_id", IntegerType).add("publish_time", StringType).add("type", IntegerType).add("ip", StringType)
    val requestDF = sqlContext.createDataFrame(requestRdd, requestStruct);
    requestDF.registerTempTable("request")

    StreamingUtils

  }

  def getValueByClickType(typeId:Int)={
    var adpageClickUv = 0
    var requestClickUv = 0
    var showUv = 0
    var downloadUv = 0
    if(typeId ==1){
      showUv =1
    }else if(typeId ==2){
      downloadUv=1
    }else if(typeId ==3){
      adpageClickUv=1
    }else if(typeId ==4){
      requestClickUv=1
    }
    Array[Int](adpageClickUv,requestClickUv,showUv,downloadUv)
  }



}
