package cn.xiaopeng.bi.utils

import java.sql.PreparedStatement

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by bigdata on 17-10-28.
  */
object DimensionUtil {

  //加载维度表数据
  def processDbDim(sqlContext: HiveContext, currentday: String): Unit = {
    //读取parquet文件
    sqlContext.read.parquet("/tmp/hive/fxdim.parquet").registerTempTable("tb_tmp")
    val parDim: DataFrame = sqlContext.sql("select medium_account,promotion_channel,promotion_mode,head_people,pkg_code from tb_tmp where date(mstart_date)<='currentday' and date(mend_date)>='currentday'".replace("currentday", currentday))
    val sql2Mysql = "update bi_gamepublic_actions set medium_account=?,promotion_channel=?,promotion_mode=?,head_people=? where publish_date=? and pkg_code=? "

    parDim.foreachPartition((rows:Iterator[Row])=>{
      val conn = JdbcUtil.getConn()
      val ps = conn.prepareStatement(sql2Mysql)
      for(x<-rows){
        ps.setString(1,x.get(0).toString)
        ps.setString(2,x.get(1).toString)
        ps.setString(3, x.get(2).toString)
        ps.setString(4, x.get(3).toString)
        ps.setString(5,currentday)
        ps.setString(6,x.get(4).toString)
        ps.executeUpdate()
      }
      ps.close()
      conn.close()
    })
  }


}
