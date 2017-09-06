package cn.xiaopeng.bi.utils

import java.sql.{Connection, PreparedStatement, Statement}

import cn.wanglei.bi.bean.MoneyMasterBean
import cn.wanglei.bi.utils.AnalysisJsonUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by kequan on 8/31/17.
  * 钱大师广告监控
  */
object AdMoneyMasterUtil {


  def main(args: Array[String]): Unit = {
    //创建各种上下文
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.default.parallelism", "60")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.sql.shuffle.partitions", "60")
    SparkUtils.setMaster(sparkConf);
    val sc = new SparkContext(sparkConf);
    val hiveContext: HiveContext = HiveContextSingleton.getInstance(sc)
    val rdd1 = sc.textFile("fill:///home/bigdata/IdeaProjects/MyXiaoPeng_BI/src/test/scala/com/xiaopeng/test/money")
    loadMoneyMasterInfo(rdd1, hiveContext)

  }


  def loadMoneyMasterInfo(rdd: RDD[String], hiveContext: HiveContext): Unit = {
    val moneyRdd: RDD[MoneyMasterBean] = rdd.filter(line => {
      (line.contains("bi_adv_money_click") || line.contains("bi_adv_money_active")) && (AnalysisJsonUtil.AnalysisMoneyMasterData(line.substring(line.indexOf("{"))) != null)
    }).map(line => {
      //map 把json解析后，返回的是bean
      AnalysisJsonUtil.AnalysisMoneyMasterData(line.substring(line.indexOf("{")))
    })
    if (!moneyRdd.isEmpty()) {
      foreachClickPartition(moneyRdd);
    }

  }


  def foreachClickPartition(moneyRdd: RDD[MoneyMasterBean]) = {
    moneyRdd.foreachPartition(iter => {
      //数据库链接
      val conn: Connection = JdbcUtil.getConn()
      val stmt: Statement = conn.createStatement()
      //一、点击相关
      //1、点击明细
      val sql_click_detail = "INSERT INTO bi_ad_money_click_detail(game_id,pkg_code,app_id,click_time,imei) VALUES (?,?,?,?,?)"
      val ps_click_detail = conn.prepareStatement(sql_click_detail)
      var params_click_detail = new ArrayBuffer[Array[Any]]()

      //2、bi_ad_money_base_day_kpi
      val sql_click_base = "INSERT INTO bi_ad_money_base_day_kpi(publish_date,child_game_id,click_num,click_dev_num)\nVALUES(?,?,?,?)\n ON DUPLICATE KEY update\nclick_num=VALUES(click_num)+click_num,\nclick_dev_num=VALUES(click_dev_num)+click_dev_num"
      val ps_click_base = conn.prepareStatement(sql_click_base)
      var params_click_base = new ArrayBuffer[Array[Any]]

      //二、激活相关
      //1、钱大师激活明细
      val sql_active_detail = "INSERT INTO bi_ad_money_active_detail(game_id,pkg_code,app_id,active_time,imei) VALUES (?,?,?,?,?) "
      val ps_active_detail = conn.prepareStatement(sql_active_detail)
      var params_active_detail = new ArrayBuffer[Array[Any]]

      //2、没有统计到的激活明细
      val sql_unactive_detail = "INSERT INTO bi_ad_money_unactive_detail(game_id,pkg_code,app_id,active_time,imei) VALUES (?,?,?,?,?) "
      val ps_unactive_detail = conn.prepareStatement(sql_unactive_detail)
      var params_unactive_detail = new ArrayBuffer[Array[Any]]()

      //3、bi_ad_money_base_day_kpi
      val sql_active_base = "INSERT INTO bi_ad_money_base_day_kpi(publish_date,child_game_id,mu_active_num,repeat_active_num,mu_repeat_active_num,pyw_mu_active_num,pyw_un_active_num,regi_active_num,new_mu_regi_dev_num)\nVALUES(?,?,?,?,?,?,?,?,?)\nON DUPLICATE KEY update mu_active_num=VALUES(mu_active_num)+mu_active_num,\nrepeat_active_num=VALUES(repeat_active_num)+repeat_active_num,\nmu_repeat_active_num=VALUES(mu_repeat_active_num)+mu_repeat_active_num,\npyw_mu_active_num=VALUES(pyw_mu_active_num)+pyw_mu_active_num,\npyw_un_active_num=VALUES(pyw_un_active_num)+pyw_un_active_num,\nregi_active_num=VALUES(regi_active_num)+regi_active_num,\nnew_mu_regi_dev_num=VALUES(new_mu_regi_dev_num)+new_mu_regi_dev_num"
      val ps_active_base = conn.prepareStatement(sql_active_base)
      var params_active_base = new ArrayBuffer[Array[Any]]()

      //每一个迭代器里面拿出来的都是bean
      iter.foreach(bean => {
        val adv_name = bean.getAdv_name
        val game_id = bean.getGame_id
        val pkg_code = bean.getPkg_id
        val app_id = bean.getApp_id
        val time = bean.getTs
        val imei = bean.getImei

        if (adv_name.equals("bi_adv_money_click")) {
          val sql_click_day = "select imei from bi_ad_money_click_detail where date(click_time)='" + time.substring(0, 10) + "' and imei='" + imei + "'"
          val rz_click_day = stmt.executeQuery(sql_click_day)
          var click_num = 0
          var click_dev_num = 0

          if (!rz_click_day.next()) {
            params_click_detail.+=(Array[Any](game_id, pkg_code, app_id, time, imei))
            click_num = 1
            click_dev_num = 1
          } else {
            click_num = click_num + 1
          }
          params_click_base.+=(Array[Any](time.substring(0, 10), game_id, click_num, click_dev_num))

          //插入数据库
          JdbcUtil.executeUpdate(ps_click_detail, params_click_detail, conn)
          JdbcUtil.executeUpdate(ps_click_base, params_click_base, conn)

        } else if (adv_name.equals("bi_adv_money_active")) {
          var mu_active_num = 0
          var repeat_active_num = 0
          var mu_repeat_active_num = 0
          var pyw_mu_active_num = 0
          var pyw_un_active_num = 0
          var regi_active_num = 0
          var new_mu_regi_dev_num = 0

          val sql_active_day = "select imei from bi_ad_money_active_detail where date(active_time)='" + time.substring(0, 10) + "' and imei='" + imei + "'";
          val rz_active_day = stmt.executeQuery(sql_active_day)
          val active_day = rz_active_day.next
          if (!active_day) {
            params_active_detail.+=(Array[Any](game_id, pkg_code, app_id, time, imei))
            mu_active_num = 1
            val sql_active_pyw_day = "select imei from bi_gamepublic_active_detail where date(active_hour)='\" + time.substring(0, 10) + \"' and imei='\" + imei + \"'"
            val rz_active_pyw_day = stmt.executeQuery(sql_active_pyw_day)
            if (rz_active_pyw_day.next()) {
              pyw_mu_active_num = 1;
            }

            val sql_active_beforetoday = "select imei from bi_ad_money_active_detail where  game_id='\" + game_id + \"'  and date(active_time)<'\" + time.substring(0, 10) + \"' and imei='\" + imei + \"'"
            val rz_active_beforetoday = stmt.executeQuery(sql_active_beforetoday)
            if (rz_active_beforetoday.next()) {
              mu_repeat_active_num = 1
            }

            val sql_active_pyw_beforetoday = "select imei from bi_gamepublic_active_detail where  game_id='\" + game_id + \"'  and  date(active_hour)<'\" + time.substring(0, 10) + \"' and imei='\" + imei + \"'"
            val rz_active_pyw_beforetoday = stmt.executeQuery(sql_active_pyw_beforetoday)
            if (rz_active_pyw_beforetoday.next()) {
              repeat_active_num = 1
            }

            val sql_active_pyw_all = "select imei from bi_gamepublic_active_detail where  game_id='" + game_id + "'and imei='" + imei + "'"
            val rz_active_pyw_all = stmt.executeQuery(sql_active_pyw_all)
            if (!rz_active_pyw_all.next()) {
              params_unactive_detail.+=(Array[Any](game_id, pkg_code, app_id, time, imei))
              pyw_un_active_num = 1
            }

            val sql_regi_day = "select * from bi_gamepublic_regi_detail where game_id='" + game_id + "'  and date(regi_hour)= '" + time.substring(0, 10) + "'and imei='" + imei + "'"
            val rz_regi_day = stmt.executeQuery(sql_regi_day)
            if (rz_regi_day.next()) {
              regi_active_num = 1
            }

            val sql_regi_beforetoday = "select * from bi_gamepublic_regi_detail where game_id='" + game_id + "'  and date(regi_hour) < '" + time.substring(0, 10) + "'and imei='" + imei + "'"
            val rz_regi_beforetoday = stmt.executeQuery(sql_regi_beforetoday)
            if (!rz_regi_beforetoday.next()) {
              new_mu_regi_dev_num = 1
            }
          }

          params_active_base.+=(Array[Any](time.substring(0, 10), game_id, mu_active_num, repeat_active_num, mu_repeat_active_num, pyw_mu_active_num, pyw_un_active_num, regi_active_num, new_mu_regi_dev_num))

          //插入数据库
          JdbcUtil.executeUpdate(ps_active_detail, params_active_detail, conn);
          JdbcUtil.executeUpdate(ps_unactive_detail, params_unactive_detail, conn);
          JdbcUtil.executeUpdate(ps_active_base, params_active_base, conn);

        }
      })
    })
  }

  //把其他实时表的数据导入到这个表
  def loadMessFromMySql() = {
    val conn = JdbcUtil.getConn()
    val stmt = conn.createStatement()
    val startday = DateUtils.getDate
    val endday = DateUtils.getTodayDate()

    val sql1 = "update bi_ad_money_base_day_kpi adkpi inner join \n(\nselect publish_date,child_game_id,sum(active_num) active_num,sum(regi_device_num) regi_device_num,sum(new_regi_device_num) new_regi_device_num from bi_gamepublic_base_opera_kpi \nwhere  publish_date>='startday' and publish_date<='endday' group by  publish_date,child_game_id\n) rs on adkpi.publish_date =rs.publish_date and adkpi.child_game_id =rs.child_game_id\nset adkpi.pyw_active_num= rs.active_num,adkpi.pyw_regi_dev_num=rs.regi_device_num,adkpi.new_regi_dev_num= rs.new_regi_device_num"
      .replace("startday", startday).replace("endday", endday);
    val sql2 = "update bi_ad_money_base_day_kpi adkpi set \nadkpi.pyw_repeat_active_num=if(adkpi.repeat_active_num-adkpi.mu_repeat_active_num>0,adkpi.repeat_active_num-adkpi.mu_repeat_active_num,0),\nadkpi.natural_regi_dev_num=if(adkpi.pyw_regi_dev_num-regi_active_num>0,adkpi.pyw_regi_dev_num-regi_active_num,0),\nadkpi.new_natural_regi_dev_num=if(adkpi.new_regi_dev_num-adkpi.new_mu_regi_dev_num>0,adkpi.new_regi_dev_num-adkpi.new_mu_regi_dev_num,0)  where publish_date>='startday' and publish_date<='endday'"
      .replace("startday", startday).replace("endday", endday);
    stmt.execute(sql1)
    stmt.execute(sql2)
    stmt.close()
    conn.close()

  }


}
