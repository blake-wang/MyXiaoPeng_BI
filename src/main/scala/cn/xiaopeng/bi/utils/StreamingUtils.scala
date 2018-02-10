package cn.xiaopeng.bi.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SaveMode}

/**
  * Created by bigdata on 17-8-1.
  */
object StreamingUtils {
  //订单日志
  def convertOrderToDfTmpTable(rdd: RDD[String], hiveContext: HiveContext) = {
    val orderRdd = rdd.filter(line => {
      val arr = line.split("\\|", -1)
      arr(0).contains("bi_order") && arr(22).contains("6") && arr(19).toInt == 4 && arr.length >= 25 && arr(2).trim.length > 0
    }).mapPartitions(it => {
      val pool = JedisUtil.getJedisPool
      val jedis = pool.getResource

      val rdd = it.map(line => {
        try {
          val splited = line.split("\\|", -1)
          var totalAmt = 0.0
          if (splited(13) != "") {
            totalAmt = java.lang.Double.parseDouble(splited(13))
          }
          jedis.select(3)
          //检查redis中是否存在已经半小时内处理的订单
          if (jedis.exists("kpi_orderexists|" + splited(2) + "|" + splited(6) + "|" + splited(5))) {
            jedis.set("kpi_orderexists|" + splited(2) + "|" + splited(6) + "|" + splited(5), "1")
            jedis.expire("kpi_orderexists|" + splited(2) + "|" + splited(6) + "|" + splited(5), 3600)
            Row(splited(5), java.lang.Integer.parseInt(splited(19)), splited(6).split(":")(0), splited(24),
              java.lang.Integer.parseInt(splited(7)), java.lang.Double.parseDouble(splited(10)),
              totalAmt, java.lang.Integer.parseInt(splited(22)))
          } else {
            Row("pyw", 0, "00-00-00 00", "", 0, 0, 0, 0)
          }
        } catch {
          case e: Exception => {
            Row("pyw", 0, "00-00-00 00", "", 0, 0, 0, 0)
          }
        }
      })
      pool.returnResource(jedis)
      pool.destroy()

      //这个返回rdd这里让人很迷惑，后面要搞清楚呀
      rdd
    })

    val orderStruct = new StructType()
      .add("game_account", StringType)
      .add("order_status", IntegerType)
      .add("publish_time", StringType)
      .add("imei", StringType)
      .add("game_id", IntegerType)
      .add("ori_price", DoubleType)
      .add("total_amt", DoubleType)
      .add("prod_type", IntegerType)

    val orderDF = hiveContext.createDataFrame(orderRdd, orderStruct)
    orderDF.registerTempTable("ods_order_tmp")


    hiveContext.sql("select o.* from ods_order_tmp o join lastPubGame pg on o.game_id=pg.game_id where o.order_status=4 and prod_type=6").cache().registerTempTable("ods_order")


  }


  /**
    * 把 全量+实时日志  深度联运转化 成 临时表
    */
  def convertPubGameLogsToDfTmpTable(rdd: RDD[String], hiveContext: HiveContext) {
    //深度联运日志,Struct,DF,temp_table
    val pubGameRdd = rdd.filter(line => line.contains("bi_pubgame")).map(line => {
      try {
        val splited = line.split("\\|", -1)
        Row(Integer.parseInt(splited(1)))
      } catch {
        case e: Exception =>
          //这里捕捉异常，并且返回-1是什么意思
          Row(-1)
      }
    })
    val pubGameStruct = new StructType().add("game_id", IntegerType)
    val pubGameDF = hiveContext.createDataFrame(pubGameRdd, pubGameStruct)
    pubGameDF.registerTempTable("pubgame")
    hiveContext.sql("use yyft")
    val lastPubGame = hiveContext.sql("select distinct t.* from (select * from pubgame union select distinct game_id from ods_publish_game) t").cache()
    lastPubGame.registerTempTable("lastPubGame")

  }

  def parseChannelToTmpTable(rdd: RDD[String], sqlContext: SQLContext) = {
    val channelRdd = rdd.filter(line => line.contains("bi_channel")).map(line => {
      try {
        val splited = line.split("\\|", -1)
        Row(StringUtils.defaultEmptyTo21(splited(2)), Integer.parseInt(splited(5)), splited(4).split(" ")(0), Integer.parseInt(splited(1)), splited(3))
      } catch {
        case e: Exception => {
          Row("pyw", 0, "0000-00-00 00", 0, "")
        }
      }
    })
    val channelStruct = new StructType()
      .add("expand_channel", StringType)
      .add("game_id", IntegerType)
      .add("publish_time", StringType)
      .add("type", IntegerType)
      .add("ip", StringType)
    val channelDF = sqlContext.createDataFrame(channelRdd, channelStruct)
    channelDF.registerTempTable("channel")
    StreamingUtils

  }

  def parseRequestToTmpTable(rdd: RDD[String], sqlContext: SQLContext) = {
    val requestRdd = rdd.filter(line => !line.contains("HTTP/1.0\" 404")).filter(line => StringUtils.isRequestLog(line, ".*0800] \"GET /Ssxy/loadComplete[?]p=[\\d|_|a-z|A-Z]+&g=[\\d]+.*"))
      .map(line => {
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
          case e: Exception => {
            Row("pyw", 0, "0000-00-00", 0, "")
          }
        }
      })
    val requestStruct = new StructType()
      .add("expand_channel", StringType)
      .add("game_id", IntegerType)
      .add("publish_time", StringType)
      .add("type", IntegerType)
      .add("ip", StringType)
    val requestDF = sqlContext.createDataFrame(requestRdd, requestStruct)
    requestDF.registerTempTable("request")

  }

  def getValueByClickType(typeId: Int) = {
    var adpageClickUv = 0
    var requestClickUv = 0
    var showUv = 0
    var downloadUv = 0
    if (typeId == 1) {
      showUv = 1
    } else if (typeId == 2) {
      downloadUv = 1
    } else if (typeId == 3) {
      adpageClickUv = 1
    } else if (typeId == 4) {
      requestClickUv = 1
    }

    Array[Int](adpageClickUv, requestClickUv, showUv, downloadUv)


  }


}
