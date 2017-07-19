package cn.xiaopeng.bi.gamepublish

import cn.xiaopeng.bi.utils.{FileUtil, JdbcUtil, SparkUtils, StringUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bigdata on 7/17/17.
  *  ltv: 用户价值分析，注册和支付
  */
object GamePublishLtvV4 {
    val logger = Logger.getLogger(GamePublishLtvV4.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val yesterday = args(0)
    //create context
    val sparkConf  = new SparkConf().setAppName(this.getClass.getName.replace("$",""))
      .set("spark.default.parallelism", "60")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.storage.memoryFraction", "0.4")
      .set("spark.streaming.stopGracefullyOnShutdown", "true");
    SparkUtils.setMaster(sparkConf)
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    //一.分析数据
    hiveContext.sql("use yyft")
    SparkUtils.readBiTable("bi_publish_back_divide",hiveContext)
    val resultSql ="select \nro.publish_date,\nro.parent_game_id,\nro.game_id,\nro.os,\nro.group_id,\nmax(ro.game_count) game_count,\nmax(ro.device_count) device_count,\nsum(if(fx.behalf_water is null,0,fx.behalf_water)) behalf_water,\nmax(if(fx.cost_date=ro.publish_date,0,fx.publish_cost)) publish_cost,\nmax(ro.ltv1_amount)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)=0 and fx.behalf_water  is not null then fx.behalf_water else 0 end) amountb1,\nmax(ro.ltv2_amount)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=1 and fx.behalf_water  is not null then fx.behalf_water else 0 end) amountb2,\nmax(ro.ltv3_amount)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=2 and fx.behalf_water  is not null then fx.behalf_water else 0 end) amountb3,\nmax(ro.ltv4_amount)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=3 and fx.behalf_water  is not null then fx.behalf_water else 0 end) amountb4,\nmax(ro.ltv5_amount)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=4 and fx.behalf_water  is not null then fx.behalf_water else 0 end) amountb5,\nmax(ro.ltv6_amount)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=5 and fx.behalf_water  is not null then fx.behalf_water else 0 end) amountb6,\nmax(ro.ltv7_amount)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=6 and fx.behalf_water  is not null then fx.behalf_water else 0 end) amountb7,\nmax(ro.ltv8_amount)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=7 and fx.behalf_water  is not null then fx.behalf_water else 0 end) amountb8,\nmax(ro.ltv9_amount)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=8 and fx.behalf_water  is not null then fx.behalf_water else 0 end) amountb9,\nmax(ro.ltv10_amount)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=9 and fx.behalf_water  is not null then fx.behalf_water else 0 end) amountb10,\nmax(ro.ltv11_amount)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=10 and fx.behalf_water  is not null then fx.behalf_water else 0 end) amountb11,\nmax(ro.ltv12_amount)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=11 and fx.behalf_water  is not null then fx.behalf_water else 0 end) amountb12,\nmax(ro.ltv13_amount)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=12 and fx.behalf_water  is not null then fx.behalf_water else 0 end) amountb13,\nmax(ro.ltv14_amount)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=13 and fx.behalf_water  is not null then fx.behalf_water else 0 end) amountb14,\nmax(ro.ltv15_amount)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=14 and fx.behalf_water  is not null then fx.behalf_water else 0 end) amountb15,\nmax(ro.ltv30_amount)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=29 and fx.behalf_water  is not null then fx.behalf_water else 0 end) amountb30,\nmax(ro.ltv45_amount)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=44 and fx.behalf_water  is not null then fx.behalf_water else 0 end) amountb45,\nmax(ro.ltv60_amount)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=59 and fx.behalf_water  is not null then fx.behalf_water else 0 end) amountb60,\nmax(ro.ltv90_amount)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=89 and fx.behalf_water  is not null then fx.behalf_water else 0 end) amountb90,\nmax(ro.ltv180_amount)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=179 and fx.behalf_water  is not null then fx.behalf_water else 0 end) amountb180,\nmax(ro.all_amount) recharge_price,\nmax(ro.all_amount)-sum(case when  fx.behalf_water  is not null then fx.behalf_water else 0 end) amount_b,\nmax(ro.ltv1_amount_divide)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)=0 and fx.behalf_water_divide  is not null then fx.behalf_water_divide else 0 end) amounta1,\nmax(ro.ltv2_amount_divide)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=1 and fx.behalf_water_divide  is not null then fx.behalf_water_divide else 0 end) amounta2,\nmax(ro.ltv3_amount_divide)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=2 and fx.behalf_water_divide  is not null then fx.behalf_water_divide else 0 end) amounta3,\nmax(ro.ltv4_amount_divide)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=3 and fx.behalf_water_divide  is not null then fx.behalf_water_divide else 0 end) amounta4,\nmax(ro.ltv5_amount_divide)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=4 and fx.behalf_water_divide  is not null then fx.behalf_water_divide else 0 end) amounta5,\nmax(ro.ltv6_amount_divide)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=5 and fx.behalf_water_divide  is not null then fx.behalf_water_divide else 0 end) amounta6,\nmax(ro.ltv7_amount_divide)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=6 and fx.behalf_water_divide  is not null then fx.behalf_water_divide else 0 end) amounta7,\nmax(ro.ltv8_amount_divide)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=7 and fx.behalf_water_divide  is not null then fx.behalf_water_divide else 0 end) amounta8,\nmax(ro.ltv9_amount_divide)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=8 and fx.behalf_water_divide  is not null then fx.behalf_water_divide else 0 end) amounta9,\nmax(ro.ltv10_amount_divide)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=9 and fx.behalf_water_divide  is not null then fx.behalf_water_divide else 0 end) amounta10,\nmax(ro.ltv11_amount_divide)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=10 and fx.behalf_water_divide  is not null then fx.behalf_water_divide else 0 end) amounta11,\nmax(ro.ltv12_amount_divide)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=11 and fx.behalf_water_divide  is not null then fx.behalf_water_divide else 0 end) amounta12,\nmax(ro.ltv13_amount_divide)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=12 and fx.behalf_water_divide  is not null then fx.behalf_water_divide else 0 end) amounta13,\nmax(ro.ltv14_amount_divide)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=13 and fx.behalf_water_divide  is not null then fx.behalf_water_divide else 0 end) amounta14,\nmax(ro.ltv15_amount_divide)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=14 and fx.behalf_water_divide  is not null then fx.behalf_water_divide else 0 end) amounta15,\nmax(ro.ltv30_amount_divide)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=29 and fx.behalf_water_divide  is not null then fx.behalf_water_divide else 0 end) amounta30,\nmax(ro.ltv45_amount_divide)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=44 and fx.behalf_water_divide  is not null then fx.behalf_water_divide else 0 end) amounta45,\nmax(ro.ltv60_amount_divide)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=59 and fx.behalf_water_divide  is not null then fx.behalf_water_divide else 0 end) amounta60,\nmax(ro.ltv90_amount_divide)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=89 and fx.behalf_water_divide  is not null then fx.behalf_water_divide else 0 end) amounta90,\nmax(ro.ltv180_amount_divide)-sum(case when datediff(to_date(fx.cost_date),ro.publish_date)<=179 and fx.behalf_water_divide  is not null then fx.behalf_water_divide else 0 end) amounta180,\nmax(ro.all_amount_divide)-sum(case when  fx.behalf_water_divide  is not null then fx.behalf_water_divide else 0 end) amount_a\nfrom\n(\n\tselect \n\tr.reg_time as publish_date,\n\tg.parent_game_id,\n\tr.game_id,\n\tg.os,\n    g.group_id,\n\tmax(r.device_count) device_count,\n\tmax(r.game_count) game_count,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)=0 and o.o_price  is not null then o.o_price else 0 end)*100 as ltv1_amount,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=1 and o.o_price is not null then o.o_price else 0 end)*100 as ltv2_amount,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=2 and o.o_price is not null then o.o_price else 0 end)*100 as ltv3_amount,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=3 and o.o_price is not null then o.o_price else 0 end)*100 as ltv4_amount,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=4 and o.o_price is not null then o.o_price else 0 end)*100 as ltv5_amount,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=5 and o.o_price is not null then o.o_price else 0 end)*100 as ltv6_amount,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=6 and o.o_price is not null then o.o_price else 0 end)*100 as ltv7_amount,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=7 and o.o_price is not null then o.o_price else 0 end)*100 as ltv8_amount,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=8 and o.o_price is not null then o.o_price else 0 end)*100 as ltv9_amount,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=9 and o.o_price is not null then o.o_price else 0 end)*100 as ltv10_amount,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=10 and o.o_price is not null then o.o_price else 0 end)*100 as ltv11_amount,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=11 and o.o_price is not null then o.o_price else 0 end)*100 as ltv12_amount,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=12 and o.o_price is not null then o.o_price else 0 end)*100 as ltv13_amount,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=13 and o.o_price is not null then o.o_price else 0 end)*100 as ltv14_amount,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=14 and o.o_price is not null then o.o_price else 0 end)*100 as ltv15_amount,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=29 and o.o_price is not null then o.o_price else 0 end)*100 as ltv30_amount,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=44 and o.o_price is not null then o.o_price else 0 end)*100 as ltv45_amount,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=59 and o.o_price is not null then o.o_price else 0 end)*100 as ltv60_amount,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=89 and o.o_price is not null then o.o_price else 0 end)*100 as ltv90_amount,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=179 and o.o_price is not null then o.o_price else 0 end)*100 as ltv180_amount,\n\tsum(case when o.o_price is not null then o.o_price else 0 end)*100 as all_amount,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)=0 and o.o_price_divide  is not null then o.o_price_divide else 0 end) as ltv1_amount_divide,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=1 and o.o_price_divide is not null then o.o_price_divide else 0 end) as ltv2_amount_divide,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=2 and o.o_price_divide is not null then o.o_price_divide else 0 end) as ltv3_amount_divide,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=3 and o.o_price_divide is not null then o.o_price_divide else 0 end) as ltv4_amount_divide,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=4 and o.o_price_divide is not null then o.o_price_divide else 0 end) as ltv5_amount_divide,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=5 and o.o_price_divide is not null then o.o_price_divide else 0 end) as ltv6_amount_divide,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=6 and o.o_price_divide is not null then o.o_price_divide else 0 end) as ltv7_amount_divide,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=7 and o.o_price_divide is not null then o.o_price_divide else 0 end) as ltv8_amount_divide,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=8 and o.o_price_divide is not null then o.o_price_divide else 0 end) as ltv9_amount_divide,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=9 and o.o_price_divide is not null then o.o_price_divide else 0 end) as ltv10_amount_divide,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=10 and o.o_price_divide is not null then o.o_price_divide else 0 end) as ltv11_amount_divide,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=11 and o.o_price_divide is not null then o.o_price_divide else 0 end) as ltv12_amount_divide,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=12 and o.o_price_divide is not null then o.o_price_divide else 0 end) as ltv13_amount_divide,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=13 and o.o_price_divide is not null then o.o_price_divide else 0 end) as ltv14_amount_divide,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=14 and o.o_price_divide is not null then o.o_price_divide else 0 end) as ltv15_amount_divide,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=29 and o.o_price_divide is not null then o.o_price_divide else 0 end) as ltv30_amount_divide,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=44 and o.o_price_divide is not null then o.o_price_divide else 0 end) as ltv45_amount_divide,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=59 and o.o_price_divide is not null then o.o_price_divide else 0 end) as ltv60_amount_divide,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=89 and o.o_price_divide is not null then o.o_price_divide else 0 end) as ltv90_amount_divide,\n\tsum(case when datediff(to_date(o.order_time),r.reg_time)<=179 and o.o_price_divide is not null then o.o_price_divide else 0 end) as ltv180_amount_divide,\n\tsum(case when  o.o_price_divide is not null then o.o_price_divide else 0 end) as all_amount_divide\n\tfrom\n\t\t(select regi.game_id game_id,regi.reg_time reg_time,count(distinct regi.imei) device_count,count(regi.game_account)  game_count from (select  game_id,imei,lower(trim(game_account)) as game_account,to_date(min(reg_time)) as reg_time from ods_regi_rz  where game_id is not null and game_account is not null and  to_date(reg_time)<='yesterday' and to_date (reg_time) >= date_add('yesterday',-180) and to_date (reg_time) >='2017-06-01' group by game_id,imei,lower(trim(game_account))) regi group by  regi.game_id, regi.reg_time)\n\t\tr\n\t\tjoin  (select distinct game_id  from ods_publish_game) ods_publish_game on r.game_id=ods_publish_game.game_id  --过滤发行游戏\n\t\tjoin  (select  distinct game_id as parent_game_id , old_game_id ,system_type as os,group_id from game_sdk) g on r.game_id=g.old_game_id --补全 main_id\n\t\tleft join\n\t\t(\n\t\tselect\n\t\tdistinct\n\t\tods_order.game_id game_id,\n\t\tto_date(ods_order.order_time) order_time,\n\t\tsum(ods_order.o_price) o_price,\n\t\tsum(ods_order.o_price*(if(bi_publish_back_divide.division_rule is null,0,bi_publish_back_divide.division_rule)))  o_price_divide \n\t\tfrom \n\t\t(select distinct order_no,order_time,lower(trim(game_account)) as game_account,game_id,(if(ori_price is null,0,ori_price)+if(total_amt is null,0,total_amt)) as o_price,payment_type from ods_order where order_status =4 and prod_type=6 and to_date(order_time)>=date_add('yesterday',-180) and to_date(order_time)<='yesterday' ) ods_order \n\t\tleft join  \n\t\t(select distinct child_game_id,divide_month,division_rule from bi_publish_back_divide where to_date(divide_month) >= to_date(concat(substr(date_add('yesterday',-180),0,7),'-01')) )bi_publish_back_divide on  ods_order.game_id=bi_publish_back_divide.child_game_id and substring(ods_order.order_time,0,7)=substring(bi_publish_back_divide.divide_month,0,7) --分成比例\n\t\tgroup by  ods_order.game_id,to_date(ods_order.order_time)\n\t\t)o on r.game_id=o.game_id\n\tgroup by r.reg_time,g.parent_game_id,r.game_id,g.os,g.group_id\n)\nro\nleft join \n(\n\tselect  \n\tdistinct\n\tfx_publish_result_base.cost_date cost_date,\n\tfx_publish_result_base.game_sub_id game_sub_id,\n\tsum(if(fx_publish_result_base.apple_proxy_recharge is null,0,fx_publish_result_base.apple_proxy_recharge)) as behalf_water, --代充流水\n\tsum(if(fx_publish_result_base.apple_proxy_recharge is null,0,fx_publish_result_base.apple_proxy_recharge) * if(bi_publish_back_divide.division_rule is null,0,bi_publish_back_divide.division_rule)/100) as behalf_water_divide, --代充流水分成后\n\tsum(if(fx_publish_result_count.publish_cost is null,0,fx_publish_result_count.publish_cost)) as publish_cost  -- 投放成本\n\tfrom \n\t(select distinct  id,game_sub_id,substring(cost_date,0,10) as cost_date,apple_proxy_recharge from fx_publish_result_base ) fx_publish_result_base \n\tjoin (select distinct result_base_id,publish_cost from fx_publish_result_count WHERE   count_status=3) fx_publish_result_count on  fx_publish_result_count.result_base_id = fx_publish_result_base.id \n\tleft join  (select distinct child_game_id,divide_month,division_rule from bi_publish_back_divide where to_date(divide_month) >= to_date(concat(substr(date_add('yesterday',-180),0,7),'-01')) )bi_publish_back_divide on  fx_publish_result_base.game_sub_id=bi_publish_back_divide.child_game_id and substring(fx_publish_result_base.cost_date ,0,7)=substring(bi_publish_back_divide.divide_month,0,7) --分成比例\n    group by fx_publish_result_base.cost_date,fx_publish_result_base.game_sub_id \n)fx on ro.game_id=fx.game_sub_id\ngroup by ro.publish_date,ro.parent_game_id,ro.game_id,ro.os,ro.group_id"
      .replace("yesterday",yesterday)
    val resultDf = hiveContext.sql(resultSql)
    FileUtil.appendDfToFile(resultDf,"./cccc.txt")

    //二.把结果存入mysql\
    resultDf.foreachPartition(rows =>{
      val sqlText = "insert into bi_gamepublic_opera_actions(publish_date,parent_game_id,child_game_id,os,group_id) values(?,?,?,?,?) on duplicate key update regi_account_num=?,regi_device_num=?,behalf_water=?,publish_cost=?,ltv_1day_b=?,ltv_2day_b=?,ltv_3day_b=?,ltv_4day_b=?,ltv_5day_b=?,ltv_6day_b=?,ltv_7day_b=?,ltv_8day_b=?,ltv_9day_b=?,ltv_10day_b=?,ltv_11day_b=?,ltv_12day_b=?,ltv_13day_b=?,ltv_14day_b=?,ltv_15day_b=?,ltv_30day_b=?,ltv_45day_b=?,ltv_60day_b=?,ltv_90day_b=?,ltv_180day_b=?,recharge_price=?,amount_b=?,ltv_1day_a=?,ltv_2day_a=?,ltv_3day_a=?,ltv_4day_a=?,ltv_5day_a=?,ltv_6day_a=?,ltv_7day_a=?,ltv_8day_a=?,ltv_9day_a=?,ltv_10day_a=?,ltv_11day_a=?,ltv_12day_a=?,ltv_13day_a=?,ltv_14day_a=?,ltv_15day_a=?,ltv_30day_a=?,ltv_45day_a=?,ltv_60day_a=?,ltv_90day_a=?,ltv_180day_a=?,amount_a=?"
      val params = new ArrayBuffer[Array[Any]]()
      for(insertedRow <- rows){
        val channelArray = StringUtils.getArrayChannel(insertedRow.get(3).toString)
        params.+=(Array[Any](insertedRow.get(0), insertedRow.get(1), insertedRow.get(2), insertedRow.get(3), insertedRow.get(4), insertedRow.get(5), insertedRow.get(6), insertedRow.get(7), insertedRow.get(8), insertedRow.get(9), insertedRow.get(10), insertedRow.get(11), insertedRow.get(12), insertedRow.get(13), insertedRow.get(14), insertedRow.get(15), insertedRow.get(16), insertedRow.get(17), insertedRow.get(18), insertedRow.get(19), insertedRow.get(20), insertedRow.get(21), insertedRow.get(22), insertedRow.get(23), insertedRow.get(24), insertedRow.get(25), insertedRow.get(26), insertedRow.get(27), insertedRow.get(28), insertedRow.get(29), insertedRow.get(30), insertedRow.get(31), insertedRow.get(32), insertedRow.get(33), insertedRow.get(34), insertedRow.get(35), insertedRow.get(36), insertedRow.get(37), insertedRow.get(38), insertedRow.get(39), insertedRow.get(40), insertedRow.get(41), insertedRow.get(42), insertedRow.get(43), insertedRow.get(44), insertedRow.get(45), insertedRow.get(46), insertedRow.get(47), insertedRow.get(48), insertedRow.get(49), insertedRow.get(50), insertedRow.get(51)))

      }
      JdbcUtil.executeBatch(sqlText,params)
    })
    sc.stop()
  }
}
