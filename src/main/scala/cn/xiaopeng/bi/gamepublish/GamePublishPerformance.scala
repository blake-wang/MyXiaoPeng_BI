package cn.xiaopeng.bi.gamepublish

import java.sql.{Connection, Date, ResultSet}

import cn.wanglei.bi.udf.{ChannelUDF, ContactDivideUDAF, GameDivideLadderUDF}
import cn.xiaopeng.bi.utils.{DateUtils, JdbcUtil, SparkUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DataType, DataTypes}

import scala.collection.mutable.ArrayBuffer


/**
  * Created by bigdata on 17-7-28.
  */
object GamePublishPerformance {
  var startDay = ""
  var endDay = ""


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    if (args.length == 2) {
      startDay = args(0)
      endDay = args(1)
    } else if (args.length == 1) {
      startDay = args(0)
      endDay = args(1)
    }
    //创建各种上下文
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""));
    SparkUtils.setMaster(sparkConf); //用来设置本地测试的
    val sc = new SparkContext(sparkConf);
    val hiveContext = new HiveContext(sc);

    hiveContext.sql("use yyft")
    //渠道分类的UDF
    hiveContext.udf.register("getchannel", new ChannelUDF(), DataTypes.StringType)
    //获取分层比例的UDF
    hiveContext.udf.register("getdivideladder", new GameDivideLadderUDF(), DataTypes.StringType)
    //连接分层金额和分层比例的UDAF
    hiveContext.udf.register("contactdivideladder", new ContactDivideUDAF())

    //第一步：全部信息不变的信息：main_id,注册信息，支付信息
    //1、全部信息
    val sql_result_rgi_order = "select\ngame_accounts_cache.performance_time as performance_time ,\ngame_sdk_cacke.parent_game_id as parent_game_id,\ngame_accounts_cache.game_id as child_game_id,\ngame_sdk_cacke.system_type as system_type,\ngame_sdk_cacke.group_id as group_id,\ngetchannel(if(ods_regi_rz_cache.expand_channel is null or ods_regi_rz_cache.expand_channel='','21',ods_regi_rz_cache.expand_channel),'0')  as  medium_channel,\ngetchannel(if(ods_regi_rz_cache.expand_channel is null or ods_regi_rz_cache.expand_channel='','21',ods_regi_rz_cache.expand_channel),'1') as ad_site_channel,\ngetchannel(if(ods_regi_rz_cache.expand_channel is null or ods_regi_rz_cache.expand_channel='','21',ods_regi_rz_cache.expand_channel),'2')  as pkg_code,\ncase when ods_order_cache.payment_type is  null  then 0 else ods_order_cache.payment_type end as pay_type,--0代表未支付\ncast(sum(if(ods_order_cache.ori_price_all is null,0,ods_order_cache.ori_price_all))*100 as int) as pay_water,\ncast(sum(if(ver.sandbox is null,0,ods_order_cache.ori_price_all))*100 as int) as box_water,\ncount(distinct case when to_date(ods_regi_rz_cache.reg_time)=game_accounts_cache.performance_time then game_accounts_cache.game_account else null end) as regi_num  from\n(select distinct  performance_time,game_account,game_id  from   (SELECT distinct to_date(ods_regi_rz.reg_time) as performance_time, lower(trim(game_account)) game_account ,game_id FROM ods_regi_rz where  to_date(ods_regi_rz.reg_time)>='startDay' and  to_date(ods_regi_rz.reg_time)<='endDay' and game_account is not null and game_id is not null  UNION all SELECT distinct to_date(ods_order.order_time) as performance_time,lower(trim(game_account)) game_account,game_id FROM ods_order where  order_status in(4,8) and  to_date(order_time)>='startDay'  and  to_date(order_time)<='endDay' and prod_type=6 and game_account is not null and game_id is not null) game_accounts_cache_per)\ngame_accounts_cache\njoin  (select distinct game_id from ods_publish_game) ods_publish_game on game_accounts_cache.game_id=ods_publish_game.game_id --过滤发行游戏\njoin  (select  distinct game_id as parent_game_id , old_game_id  ,system_type,group_id from game_sdk  where state=0) game_sdk_cacke on game_accounts_cache.game_id=game_sdk_cacke.old_game_id --补全 parent_game_id\njoin  (select  distinct lower(trim(game_account)) game_account, to_date(reg_time) reg_time,expand_channel from ods_regi_rz where game_id is not null and game_account is not null and reg_time is not null)  ods_regi_rz_cache  on ods_regi_rz_cache.game_account=game_accounts_cache.game_account--注册信息\nleft join  (select distinct order_no,order_time, lower(trim(game_account)) game_account,game_id,(if(ori_price is null,0,ori_price)+if(total_amt is null,0,total_amt)) as ori_price_all,payment_type,order_status from ods_order where order_status=4 and  to_date(order_time)>='startDay' and   to_date(order_time)<='endDay' and prod_type=6) ods_order_cache on  ods_order_cache.game_id=game_accounts_cache.game_id and  ods_order_cache.game_account=game_accounts_cache.game_account and game_accounts_cache.performance_time=to_date(ods_order_cache.order_time) --支付信息\nleft join  (select order_sn,sandbox from pywsdk_apple_receipt_verify where sandbox=1 and state=3) ver on ver.order_sn=ods_order_cache.order_no\ngroup by game_accounts_cache.performance_time,game_sdk_cacke.parent_game_id,game_accounts_cache.game_id,game_sdk_cacke.system_type,game_sdk_cacke.group_id,if(ods_regi_rz_cache.expand_channel is null or ods_regi_rz_cache.expand_channel='','21',ods_regi_rz_cache.expand_channel),ods_order_cache.payment_type"
      .replace("startDay", startDay).replace("endDay", endDay);
    val df_result_rgi_order = hiveContext.sql(sql_result_rgi_order).persist()
    df_result_rgi_order.registerTempTable("result_rgi_order")

    //第二步 获取关联信息
    //bi_publish_back_cost 由  pkg_code  决定的字段 1个(main_man)
    val sql_result_pkg_code_pre = "select distinct\nresult_rgi_order_cache.performance_time as performance_time,\nresult_rgi_order_cache.pkg_code as pkg_code,\ncase \nwhen users_channel.name_user_channel is not null  then users_channel.name_user_channel\nwhen users_medium.name_user_medium is not null  then users_medium.name_user_medium \nelse '' end as main_man\nfrom \n(select distinct pkg_code,performance_time from  result_rgi_order) \nresult_rgi_order_cache \nleft join  (select pkg_code , channel_id from channel_pkg)  channel_pkg  on result_rgi_order_cache.pkg_code=channel_pkg.pkg_code --渠道负责人1\nleft join  (select channel_id , manager,start_time,end_time from channel_manager) channel_manager on  channel_pkg.channel_id = channel_manager.channel_id  and   result_rgi_order_cache.performance_time >= to_date(channel_manager.start_time)  and  result_rgi_order_cache.performance_time < to_date(channel_manager.end_time)--渠道负责人2\nleft join  (select id as id_user_channel , name as name_user_channel from user) users_channel on  users_channel.id_user_channel = channel_manager.manager--渠道负责人3\nleft join  (select subpackage_id , merchant_id from medium_package) medium_package on result_rgi_order_cache.pkg_code=medium_package.subpackage_id--媒介账号负责人1\nleft join  (select merchant_id as merchant_id_mer , id from merchant) merchant on  merchant.merchant_id_mer =medium_package.merchant_id--媒介账号负责人2\nleft join  (select  source_id,user_id,start_time,end_time  from charge_log where family=3) charge_log on charge_log.source_id=merchant.id  and   result_rgi_order_cache.performance_time >= to_date(charge_log.start_time)  and  result_rgi_order_cache.performance_time < to_date(charge_log.end_time)--媒介账号负责人3\nleft join  (select  id as id_user_medium , name as name_user_medium from user) users_medium on users_medium.id_user_medium=charge_log.user_id--媒介账号负责人4"
    hiveContext.sql(sql_result_pkg_code_pre).registerTempTable("result_pkg_code")
    val sql_result_pkg_code = "select distinct performance_time,pkg_code,main_man from result_pkg_code where main_man!=''"
    val df_result_pkg_code = hiveContext.sql(sql_result_pkg_code).registerTempTable("result_pkg_code")

    //第三步 关联信息 并存入数据库
    resetData()
    //重置数据
    //1、存入 bi_publish_back_performance
    val bi_publish_back_performance = "select \nresult_rgi_order_cache.performance_time performance_time,\nresult_rgi_order_cache.medium_channel medium_channel,\nresult_rgi_order_cache.ad_site_channel ad_site_channel,\nresult_rgi_order_cache.pkg_code pkg_code,\nresult_rgi_order_cache.parent_game_id parent_game_id,\nresult_rgi_order_cache.child_game_id child_game_id,\nresult_rgi_order_cache.pay_water pay_water,\nresult_rgi_order_cache.box_water box_water,\nresult_rgi_order_cache.regi_num regi_num,\nresult_rgi_order_cache.pay_type pay_type,\nif(result_pkg_code_cache.main_man is null,'',result_pkg_code_cache.main_man) main_man\nfrom\n(select distinct performance_time,medium_channel,ad_site_channel,pkg_code,parent_game_id,box_water,child_game_id,pay_water,regi_num,pay_type from  result_rgi_order)result_rgi_order_cache\nleft join (select  performance_time,pkg_code,max(main_man) main_man from result_pkg_code group by  performance_time,pkg_code ) result_pkg_code_cache on result_pkg_code_cache.performance_time=result_rgi_order_cache.performance_time and result_pkg_code_cache.pkg_code=result_rgi_order_cache.pkg_code"
    val df_bi_publish_back_performance = hiveContext.sql(bi_publish_back_performance)
    updateToMysql(df_bi_publish_back_performance, "result_rgi_order_performance")

    //2、存入 bi_publish_back_cost
    val bi_publish_back_cost = "select distinct\nresult_rgi_order_cache.performance_time,\nresult_rgi_order_cache.parent_game_id,\nresult_rgi_order_cache.child_game_id,\nresult_rgi_order_cache.system_type system_type,\nresult_rgi_order_cache.group_id group_id,\nif(corporation_finance.bill_type is null,0,corporation_finance.bill_type) bill_type,\nif(game_base.corporation_id is null,0,game_base.corporation_id) corporation_id\nfrom\n(select distinct performance_time,parent_game_id,child_game_id,system_type,group_id from  result_rgi_order) result_rgi_order_cache\nleft join (select distinct corporation_id,id from game_base) game_base on  result_rgi_order_cache.parent_game_id = game_base.id  --cp名称id ,发票类型 1 \nleft join (select distinct bill_type,corporation_base_id from corporation_finance) corporation_finance on  corporation_finance.corporation_base_id = game_base.corporation_id --cp名称,发票类型2 \n"
    val df_bi_publish_back_cost = hiveContext.sql(bi_publish_back_cost)
    updateToMysql(df_bi_publish_back_cost, "result_rgi_order_cost")

    //分成比例 动态影响因素 ： 1、流水  2、带冲成本
    //cp_cost 动态影响因素  分层比例 ===> 1、流水 2、代冲成本
    //第四步，更新我方分层比  流水因素
    //读取流水
    SparkUtils.readBiTable("bi_publish_back_performance", hiveContext)
    val df_pay_water_day: DataFrame =hiveContext.sql("select performance_time, parent_game_id, child_game_id, sum(pay_water) pay_water, sum(if(pay_type=5,pay_water,0)) apple_water from bi_publish_back_performance group by performance_time,parent_game_id,child_game_id")
    df_pay_water_day.registerTempTable("pay_water_day")

    //1.跟随主游戏
//    val sql_order_month_parent =
  }

  /*
  * 重置数据
  * */
  def resetData(): Unit = {
    val conn: Connection = JdbcUtil.getConn()
    val stmt = conn.createStatement()
    var sql1 = "delete from bi_publish_back_performance where performance_time>='startDay' and performance_time<='endDay'".replace("startDay", startDay).replace("endDay", endDay)
    var sql2 = "delete from bi_publish_back_cost where performance_time>='startDay' and performance_time<='endDay' ".replace("startDay", startDay).replace("endDay", endDay)
    stmt.execute(sql1)
    stmt.execute(sql2)
    stmt.close()
    conn.close()
  }

  def updateToMysql(df: DataFrame, result: String) = {
    df.foreachPartition(iter => {
      //数据库连接
      val conn = JdbcUtil.getConn()
      val stmtBi = conn.createStatement()

      var sql = ""
      if (result.equals("result_rgi_order_performance")) {
        sql = "INSERT INTO bi_publish_back_performance(performance_time,medium_channel,ad_site_channel,pkg_code,parent_game_id,child_game_id,pay_water,box_water,regi_num,pay_type,main_man) VALUES (?,?,?,?,?,?,?,?,?,?,?)"
      } else if (result.equals("result_rgi_order_cost")) {
        sql = "INSERT INTO bi_publish_back_cost(performance_time,parent_game_id,child_game_id,system_type,group_id,bill_type,corporation_id) VALUES (?,?,?,?,?,?,?)"
      } else if (result.equals("result_divide_all")) {
        sql = "INSERT INTO bi_publish_back_divide(parent_game_id,child_game_id,divide_month,performance_date,division_rule) VALUES (?,?,?,?,?) ON DUPLICATE KEY update performance_date= VALUES(performance_date),division_rule= VALUES(division_rule)"
      } else if (result.equals("result_cost")) {
        sql = "update bi_publish_back_cost set behalf_water=?,behalf_cost=?,publish_cost=?,cp_cost=?,profit=? where performance_time=? and child_game_id=?";
      }

      val yesterday = DateUtils.getYesterDayDate()

      val params = new ArrayBuffer[Array[Any]]
      iter.foreach(t => {
        if (result.equals("result_rgi_order_performance")) {
          if (t.getAs[String]("medium_channel").length <= 10 && t.getAs[String]("ad_site_channel").length <= 10 && t.getAs[String]("pkg_code").length <= 12) {
            var pay_type: String = t.getAs[String]("pay_type")
            if (pay_type.toInt == 11) {
              pay_type = "a"
            } else if (pay_type.toInt == 100) {
              pay_type = "b"
            }
            params.+=(Array[Any](t.getAs[Date]("performance_time"), t.getAs[String]("medium_channel"), t.getAs[String]("ad_site_channel"), t.getAs[String]("pkg_code"), t.getAs[Int]("parent_game_id"), t.getAs[Int]("child_game_id"), t.getAs[Int]("pay_water"), t.getAs[Int]("box_water"), t.getAs[Long]("regi_num").toInt, pay_type, t.getAs[String]("main_man")));
          } else {
            Logger.getLogger(GamePublishPerformance.getClass).error("expand_channel format error::::: " + t.toString())
          }
        } else if (result.equals("result_rgi_order_cost")) {
          params.+=(Array[Any](t.getAs[Date]("performance_time"), t.getAs[Int]("parent_game_id"), t.getAs[Int]("child_game_id"), t.getAs[Byte]("system_type"), t.getAs[Int]("group_id"), t.getAs[Byte]("bill_type"), t.getAs[Int]("corporation_id")))
        } else if (result.equals("result_divide_all")) {
          val sql_divide = "update bi_publish_back_cost set slotting_fee='"
          +(t.getAs[Long]("slotting_fee")).toInt / 100 + "',division_rule='" + (t.getAs[String]("division_rule").toInt / 100).toByte + "' where  left(performance_time,7)='" + t.getAs[String]("performance_month") + "' and child_game_id='" + t.getAs[Int]("child_game_id") + "' and division_rule!='" + (t.getAs[String]("division_rule").toInt / 100).toByte + "'";
          val result_change: Int = stmtBi.executeUpdate(sql_divide)
          if (result_change >= 1) {
            val sql_divide_de = "SELECT distinct division_rule FROM bi_publish_back_divide WHERE child_game_id='" + t.getAs[Int]("child_game_id") + "' and divide_month='" + t.getAs[String]("performance_month") + "'"
            val rz_divide: ResultSet = stmtBi.executeQuery(sql_divide_de)
            if (rz_divide.next()) {
              val division_rule = rz_divide.getString("division_rule")
              if (division_rule != (t.getAs[String]("division_rule").toInt / 100).toByte) {
                //分成比例发生改变
                params.+=(Array[Any](t.getAs[Int]("parent_game_id"), t.getAs[Int]("child_game_id"), t.getAs[String]("performance_month").toString.substring(0, 7) + "-01", yesterday, (t.getAs[String]("division_rule").toInt / 100).toByte))
              }
              while (rz_divide.next()) {
                if (division_rule != (t.getAs[String]("division_rule").toInt / 100).toByte) {
                  //分成比例发生改变
                  params.+=(Array[Any](t.getAs[Int]("parent_game_id"), t.getAs[Int]("child_game_id"), t.getAs[String]("performance_month").toString.substring(0, 7) + "-01", yesterday, (t.getAs[String]("division_rule").toInt / 100).toByte))
                }
              }
            } else {
              //分成比例发生改变
              params.+=(Array[Any](t.getAs[Int]("parent_game_id"), t.getAs[Int]("child_game_id"), t.getAs[String]("performance_month").toString.substring(0, 7) + "-01", yesterday, (t.getAs[String]("division_rule").toInt / 100).toByte))
            }
          }
        } else if (result.equals("result_cost")) {
          params.+=(Array[Any](t.getAs[Int]("behalf_water"), t.getAs[Int]("behalf_cost"), t.getAs[Int]("publish_cost"), Math.round(t.getAs[Double]("cp_cost")).toInt, Math.round(t.getAs[Double]("profit")).toInt, t.getAs[Date]("performance_time"), t.getAs[Int]("child_game_id")));

        }
      })
      JdbcUtil.executeBatch(sql, params)
    })

  }
}
