package cn.xiaopeng.bi.gamepublish

import cn.wanglei.bi.udf.ChannelUDF
import cn.xiaopeng.bi.utils.SparkUtils
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by bigdata on 17-7-26.
  * 注册，激活，登录，支付，离线
  */
object GamePublishKpiOffline {
  var startTime = ""
  var endTime = ""
  var mode = ""

  def main(args: Array[String]): Unit = {
    startTime = args(0)
    endTime = args(1)
    if (args.length > 2) {
      mode = args(2)
    }
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.default.parallelism", "60")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.storage.memoryFraction", "0.4")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    SparkUtils.setMaster(sparkConf)
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    hiveContext.udf.register("getchannel",new ChannelUDF(),DataTypes.StringType)
    hiveContext.sql("use yyft")
    if(mode.equals("regi")){
      //注册相关
//      GamePublicUtil3
    }
  }

}
