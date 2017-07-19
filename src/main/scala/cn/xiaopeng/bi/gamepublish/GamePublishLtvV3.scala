package cn.xiaopeng.bi.gamepublish

import cn.xiaopeng.bi.utils.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.log4j.spi.LoggerFactory
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by JSJSB-0071 on 2017/7/13.
 */
object GamePublishLtvV3 {
  val logger = Logger.getLogger(GamePublishLtvV3.getClass)

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val yesterday = args(0)
    //创建各种上下文

    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.default.parallelism", "60")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.storage.memoryFraction", "0.4")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")

    SparkUtils.setMaster(sparkConf)
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    hiveContext.sql("use yyft")
    val resultSql = ("select \n" +
      "ods_regi_rz_cache.reg_time as publish_date,\n" +
      "game_sdk_cache.parent_game_id,\n" +
      "ods_regi_rz_cache.game_id,\n" +
      "if(ods_regi_rz_cache.expand_channel is null or ods_regi_rz_cache.expand_channel='','21',ods_regi_rz_cache.expand_channel) as expand_channel,\n" +
      "game_sdk_cache.os,\n" +
      "game_sdk_cache.group_id,\n" +
      "count(distinct ods_regi_rz_cache.game_account) as acc_add_regi,\n" +
      "sum(case when datediff(to_date(ods_order_cache.order_time),ods_regi_rz_cache.reg_time)=0 and ods_order_cache.ori_price_all  " +
      "is not null then if(ods_order_cache.order_status = 4,ods_order_cache.ori_price_all,-ods_order_cache.ori_price_all) else 0 end)*100 as ltv1_amount,\n" +
      "sum(case when datediff(to_date(ods_order_cache.order_time),ods_regi_rz_cache.reg_time)<=1 and ods_order_cache.ori_price_all is not null " +
      "then if(ods_order_cache.order_status = 4,ods_order_cache.ori_price_all,-ods_order_cache.ori_price_all) else 0 end)*100 as ltv2_amount,\n" +
      "sum(case when datediff(to_date(ods_order_cache.order_time),ods_regi_rz_cache.reg_time)<=2 and ods_order_cache.ori_price_all is not null " +
      "then if(ods_order_cache.order_status = 4,ods_order_cache.ori_price_all,-ods_order_cache.ori_price_all) else 0 end)*100 as ltv3_amount,\n" +
      "sum(case when datediff(to_date(ods_order_cache.order_time),ods_regi_rz_cache.reg_time)<=3 and ods_order_cache.ori_price_all is not null " +
      "then if(ods_order_cache.order_status = 4,ods_order_cache.ori_price_all,-ods_order_cache.ori_price_all) else 0 end)*100 as ltv4_amount,\n" +
      "sum(case when datediff(to_date(ods_order_cache.order_time),ods_regi_rz_cache.reg_time)<=4 and ods_order_cache.ori_price_all is not null " +
      "then if(ods_order_cache.order_status = 4,ods_order_cache.ori_price_all,-ods_order_cache.ori_price_all) else 0 end)*100 as ltv5_amount,\n" +
      "sum(case when datediff(to_date(ods_order_cache.order_time),ods_regi_rz_cache.reg_time)<=5 and ods_order_cache.ori_price_all is not null " +
      "then if(ods_order_cache.order_status = 4,ods_order_cache.ori_price_all,-ods_order_cache.ori_price_all) else 0 end)*100 as ltv6_amount,\n" +
      "sum(case when datediff(to_date(ods_order_cache.order_time),ods_regi_rz_cache.reg_time)<=6 and ods_order_cache.ori_price_all is not null " +
      "then if(ods_order_cache.order_status = 4,ods_order_cache.ori_price_all,-ods_order_cache.ori_price_all) else 0 end)*100 as ltv7_amount,\n" +
      "sum(case when datediff(to_date(ods_order_cache.order_time),ods_regi_rz_cache.reg_time)<=7 and ods_order_cache.ori_price_all is not null " +
      "then if(ods_order_cache.order_status = 4,ods_order_cache.ori_price_all,-ods_order_cache.ori_price_all) else 0 end)*100 as ltv8_amount,\n" +
      "sum(case when datediff(to_date(ods_order_cache.order_time),ods_regi_rz_cache.reg_time)<=8 and ods_order_cache.ori_price_all is not null " +
      "then if(ods_order_cache.order_status = 4,ods_order_cache.ori_price_all,-ods_order_cache.ori_price_all) else 0 end)*100 as ltv9_amount,\n" +
      "sum(case when datediff(to_date(ods_order_cache.order_time),ods_regi_rz_cache.reg_time)<=9 and ods_order_cache.ori_price_all is not null " +
      "then if(ods_order_cache.order_status = 4,ods_order_cache.ori_price_all,-ods_order_cache.ori_price_all) else 0 end)*100 as ltv10_amount,\n" +
      "sum(case when datediff(to_date(ods_order_cache.order_time),ods_regi_rz_cache.reg_time)<=10 and ods_order_cache.ori_price_all is not null " +
      "then if(ods_order_cache.order_status = 4,ods_order_cache.ori_price_all,-ods_order_cache.ori_price_all) else 0 end)*100 as ltv11_amount,\n" +
      "sum(case when datediff(to_date(ods_order_cache.order_time),ods_regi_rz_cache.reg_time)<=11 and ods_order_cache.ori_price_all is not null " +
      "then if(ods_order_cache.order_status = 4,ods_order_cache.ori_price_all,-ods_order_cache.ori_price_all) else 0 end)*100 as ltv12_amount,\n" +
      "sum(case when datediff(to_date(ods_order_cache.order_time),ods_regi_rz_cache.reg_time)<=12 and ods_order_cache.ori_price_all is not null " +
      "then if(ods_order_cache.order_status = 4,ods_order_cache.ori_price_all,-ods_order_cache.ori_price_all) else 0 end)*100 as ltv13_amount,\n" +
      "sum(case when datediff(to_date(ods_order_cache.order_time),ods_regi_rz_cache.reg_time)<=13 and ods_order_cache.ori_price_all is not null " +
      "then if(ods_order_cache.order_status = 4,ods_order_cache.ori_price_all,-ods_order_cache.ori_price_all) else 0 end)*100 as ltv14_amount,\n" +
      "sum(case when datediff(to_date(ods_order_cache.order_time),ods_regi_rz_cache.reg_time)<=14 and ods_order_cache.ori_price_all is not null " +
      "then if(ods_order_cache.order_status = 4,ods_order_cache.ori_price_all,-ods_order_cache.ori_price_all) else 0 end)*100 as ltv15_amount,\n" +
      "sum(case when datediff(to_date(ods_order_cache.order_time),ods_regi_rz_cache.reg_time)<=29 and ods_order_cache.ori_price_all is not null " +
      "then if(ods_order_cache.order_status = 4,ods_order_cache.ori_price_all,-ods_order_cache.ori_price_all) else 0 end)*100 as ltv30_amount,\n" +
      "sum(case when datediff(to_date(ods_order_cache.order_time),ods_regi_rz_cache.reg_time)<=44 and ods_order_cache.ori_price_all is not null " +
      "then if(ods_order_cache.order_status = 4,ods_order_cache.ori_price_all,-ods_order_cache.ori_price_all) else 0 end)*100 as ltv45_amount,\n" +
      "sum(case when datediff(to_date(ods_order_cache.order_time),ods_regi_rz_cache.reg_time)<=59 and ods_order_cache.ori_price_all is not null " +
      "then if(ods_order_cache.order_status = 4,ods_order_cache.ori_price_all,-ods_order_cache.ori_price_all) else 0 end)*100 as ltv60_amount,\n" +
      "sum(case when datediff(to_date(ods_order_cache.order_time),ods_regi_rz_cache.reg_time)<=89 and ods_order_cache.ori_price_all is not null " +
      "then if(ods_order_cache.order_status = 4,ods_order_cache.ori_price_all,-ods_order_cache.ori_price_all) else 0 end)*100 as ltv90_amount,\n" +
      "sum(case when datediff(to_date(ods_order_cache.order_time),ods_regi_rz_cache.reg_time)<=179 and ods_order_cache.ori_price_all is not null " +
      "then if(ods_order_cache.order_status = 4,ods_order_cache.ori_price_all,-ods_order_cache.ori_price_all) else 0 end)*100 as ltv180_amount\n" +
      "from\n" +
      "(select lower(trim(game_account)) as game_account,game_id,to_date(min(reg_time)) as reg_time,max(expand_channel) as expand_channel " +
      "from " +
      "ods_regi_rz where game_id is not null and game_account is not null and  " +
      "to_date(reg_time)<='yesterday' and to_date (reg_time) >= date_add('yesterday',-180) group by lower(trim(game_account)),game_id)  ods_regi_rz_cache\n" +
      "join  (select distinct game_id  from ods_publish_game) ods_publish_game on ods_regi_rz_cache.game_id=ods_publish_game.game_id  --过滤发行游戏\n" +
      "join  (select  distinct game_id as parent_game_id , old_game_id as child_game_id,system_type as os,group_id from game_sdk) game_sdk_cache on ods_regi_rz_cache.game_id=game_sdk_cache.child_game_id --补全 main_id\n" +
      "left  join  (select distinct order_no,order_time,lower(trim(game_account)) as game_account,game_id,(if(ori_price is null,0,ori_price)+if(total_amt is null,0,total_amt)) as ori_price_all,total_amt,payment_type,order_status from ods_order where order_status in(4,8) and prod_type=6 and to_date(order_time)>=date_add('yesterday',-180)) ods_order_cache on  ods_order_cache.game_account=ods_regi_rz_cache.game_account--支付信息 \n" +
      "group by ods_regi_rz_cache.reg_time,game_sdk_cache.parent_game_id,ods_regi_rz_cache.game_id,if(ods_regi_rz_cache.expand_channel is null or ods_regi_rz_cache.expand_channel='','21',ods_regi_rz_cache.expand_channel),game_sdk_cache.os,game_sdk_cache.group_id")
      .replace("yesterday", yesterday)

    val resultDF = hiveContext.sql(resultSql)


    resultDF.foreachPartition(rows =>{

    })



  }
}
