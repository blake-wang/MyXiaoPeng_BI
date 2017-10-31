package cn.xiaopeng.bi.utils

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by bigdata on 17-10-26.
  */
object GamePublish2TmpDim {



  def main(args: Array[String]): Unit = {
    creatDbDim()
  }

  def creatDbDim() = {
    val tmpSql = "SELECT mpk.subpackage_id as pkg_code,ag.agent_name as promotion_channel,mc.merchant as medium_account,'' as promotion_mode ,'2016-01-01' mstart_date,'9999-01-01' as mend_date,\nus.name as head_people,to_date('2016-01-01') as astart_date,to_date('9999-01-01') aend_date\nfrom medium_package mpk \nleft join merchant mc on mc.merchant_id=mpk.merchant_id \nleft join agent ag on ag.id= mc.agent_id \nleft join `user` as us on us.id=mpk.user_id \n--渠道商\nunion \nSELECT pkg.pkg_code,cm.main_name promotion_channel,'' as medium_account,cpc.promotion promotion_mode,to_date(cpc.start_time) mstart_time,to_date(cpc.end_time) mend_time,\nus.name,to_date('2016-01-01'),to_date('9999-01-01')\nFROM channel_pkg pkg \nleft join channel_main cm on cm.id=pkg.main_id \nleft join channel_pkg_conf cpc on cpc.pkg_id=pkg.id\nleft  join `user` as us on us.id=pkg.manager \norder by pkg_code"
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$",""))
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use yyft")
    val df = sqlContext.sql(tmpSql)
    df.coalesce(20)
    df.write.mode(SaveMode.Overwrite).save("/tmp/hive/fxdim.parquet")
  }
}
