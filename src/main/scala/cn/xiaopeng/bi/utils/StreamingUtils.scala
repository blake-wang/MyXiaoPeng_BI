package cn.xiaopeng.bi.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.{Row, SQLContext}

/**
  * Created by bigdata on 17-8-1.
  */
object StreamingUtils {

  def convertPubGameLogsToDfTmpTable(rdd: RDD[String], sqlContext: SQLContext) {
    //深度联运日志
    val pubGameRdd = rdd.filter(line => line.contains("bi_pubgame")).map(line => {
      try {
        val splited = line.split("\\|", -1)
        Row(Integer.parseInt(splited(1)))
      } catch {
        case ex:Exception =>{
          Row(-1)
        }
      }
    })
    val pubGameStruct = (new StructType()).add("game_id",IntegerType)
    val pubGameDF = sqlContext.createDataFrame(pubGameRdd,pubGameStruct)
    pubGameDF.registerTempTable("pubgame")
    val lastPubGame = sqlContext.sql("select distinct t.* from (select * from pubgame union select distinct game_id from yyft.ods_publish_game) t")
    lastPubGame.registerTempTable("lastPubGame")
  }

}
