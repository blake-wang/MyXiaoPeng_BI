package cn.xiaopeng.bi.utils

import org.apache.spark.rdd.RDD

/**
  * Created by bigdata on 17-11-3.
  */
object GamePublicLoginUtil {

  def loadLoginInfo(dslogs: RDD[String]) = {
    val rdd = dslogs.filter(x => {
      val lgInfo = x.split("\\|", -1)
      lgInfo(0).contains("bi_login") && lgInfo.length >= 9 && StringUtils.isNumber(lgInfo(8))
    }).map(x => {
      val lgInfo = x.split("\\|", -1)
      (lgInfo(3).trim.toLowerCase(), lgInfo(4), lgInfo(8).toInt, if (lgInfo(6).equals("") || lgInfo(6) == null) "21" else lgInfo(6), lgInfo(7))
    })
    rdd.foreachPartition(fp => {
      loginActions(fp)
    })
    JedisPoolSingleton.destroy()
  }

  /**
    * 用户登录行为
    * game_account(3),logintime(4),game_id(8)，expand_channel(6),imei(7)
    *
    * @param fp
    */
  def loginActions(fp: Iterator[(String, String, Int, String, String)]): Unit = {
    val pool = JedisPoolSingleton.getJedisPool
    //通过不同的jedis对象来选择不同的库，这样可以区分存储的数据
    val jedis0 = pool.getResource
    jedis0.select(0)
    val jedis3 = pool.getResource
    jedis3.select(3)

    val conn = JdbcUtil.getConn()
    val connFx = JdbcUtil.getXiaopeng2FXConn()
    for(row<-fp){
      //取出每一行数据，进行处理

    }

  }


}
