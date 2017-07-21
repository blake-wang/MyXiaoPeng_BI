package cn.xiaopeng.bi.utils

import java.sql.Connection

import org.apache.spark.rdd.RDD
import redis.clients.jedis.{Jedis, JedisPool}

/**
 * Created by JSJSB-0071 on 2017/7/13.
 */
object GamePublicActs {
  /*
  *
  * 加载订单数据
  * 参数：游戏账号（5），订单号（2），订单日期（6），游戏id（7）,充值流水（10）+代金券，imei(24).sub
  * */
  def loadOrderInfo(dslogs: RDD[String]) = {
    val rdd = dslogs.filter(x => {
      val arr = x.split("\\|")
      //排除截断日志   只取存在订单号的数据，不存在订单号的为代金券消费  只取直充  只取状态为4的数据
      arr(0).contains("bi_order") && arr.length >= 25 && arr(2).trim.length > 0 && arr(22).contains("6") && arr(19).toInt == 45
    }).map(x=>{
      val odInfo = x.split("\\|")
      (odInfo(5).trim.toLowerCase,odInfo(2),odInfo(6),odInfo(7),Commons.getNullTo0(odInfo(10))+Commons.getNullTo0(odInfo(13)),odInfo(24))

    })
    rdd.foreachPartition(fp=>{
      orderActions(fp)
    })
  }
  /**
    * 用户消费
    *
    * @param fp
    * 参数：游戏账号（5），订单号（2），订单日期（6），游戏id（7）,充值流水（10），imei(24).sub
    */

  def orderActions(fp:Iterator[(String,String,String,String,Float,String)]):Unit={
    val pool:JedisPool = JedisPoolSingleton.getJedisPool()
    val jedis0:Jedis = pool.getResource
    jedis0.select(0)
    val jedis3 = pool.getResource
    jedis3.select(3)
    val conn:Connection = JdbcUtil.getConn()
    for (row <- fp){
      if(!jedis3.exists(row._1+"|"+row._2+"|"+row._3+"|"+"_publishOrderActions")){
        val game_account = row._1
        var expand_channel = jedis0.hget(row._1,"expand_channel")
        if(expand_channel==null||expand_channel.equals("")||expand_channel.equals("0")) expand_channel = "21"
        val imei = row._6
        val game_id = row._4.toInt
        val ori_price = row._5
        val order_date = row._3.substring(0,10)
        var parent_game_id = jedis0.hget(game_id.toString()+"_publish_game","mainid")
        if(parent_game_id==null) parent_game_id = "0"
        val medium_channel = StringUtils.getArrayChannel(expand_channel)(0)
        val ad_site_channel = StringUtils.getArrayChannel(expand_channel)(1)
        val pkg_code = StringUtils.getArrayChannel(expand_channel)(2)
        var medium_account = jedis0.hget(pkg_code+"_pkgcode","medium_account")
        if(medium_account == null) medium_account=""
        var promotion_channel = jedis0.hget(pkg_code + "_pkgcode", "promotion_channel")
        if(promotion_channel ==null) promotion_channel = ""
        var promotion_mode = jedis0.hget(pkg_code + "_" + order_date + "_pkgcode", "promotion_mode")
        if(promotion_mode==null) promotion_mode = ""
        var head_people = jedis0.hget(pkg_code + "_" + order_date + "_pkgcode", "head_people")
        if(head_people==null) head_people=""
        var os = jedis0.hget(game_id.toString+"_publish_game","system_type")
        if(os ==null) os = "1"
        val groupid =  jedis0.hget(game_id+"_publish_game","publish_group_id")
        val group_id = if(groupid == null) 0 else groupid.toInt
        /*计算指标*/
        val isNewRegiDevDay = GamePublicDao.isNewRegiDevDay(pkg_code, imei, order_date, medium_channel, ad_site_channel,game_id, conn)
        //是否为当日新增注册设备
        //是否为新增注册设备
        val isNewRegiAccountDay = GamePublicDao.isNewRegiAccountDay

      }
    }
  }



}
