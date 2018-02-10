package cn.xiaopeng.bi.utils.action

import cn.xiaopeng.bi.utils._
import org.apache.spark.rdd.RDD

/**
  * Created by bigdata on 18-2-9.
  */
object GamePublicActs2 {


  /**
    * 加载登录数据
    *
    * @param dslogs
    */
  def loadLoginInfo(dslogs: RDD[String]): Unit = {
    val rdd = dslogs.filter(x => {
      val lgInfo = x.split("\\|", -1)
      lgInfo(0).contains("bi_login") && lgInfo.length >= 9 && StringUtils.isNumber(lgInfo(8))
    }).map(x => {
      val lgInfo = x.split("\\|", -1)
      (lgInfo(3).trim.toLowerCase(), lgInfo(4), lgInfo(8).toInt, if (lgInfo(6).equals("") || lgInfo(6) == null) "21" else lgInfo(6), lgInfo(7))
    })
    rdd.foreachPartition(fp => {
      if (!fp.isEmpty) {
        loginActions(fp)
      }
    })

  }

  def loadOrderInfo(rdd: RDD[String]): Unit = {

  }


  def loginActions(fp: Iterator[(String, String, Int, String, String)]) = {
    val pool = JedisUtil.getJedisPool

    val jedis0 = pool.getResource
    jedis0.select(0)

    val jedis3 = pool.getResource
    jedis3.select(3)

    val conn = JdbcUtil.getConn()
    val connFx = JdbcUtil.getXiaopeng2FXConn()

    for (row <- fp) {
      //重复日志去重
      if (!jedis3.exists(row._1 + "|" + row._2 + "|" + "_publicLoginActions")) {
        //判断是否为发行游戏，只取发行游戏
        if (jedis0.exists(row._3.toString + "_publish_game")) {
          val game_id = row._3.toInt
          val game_account = row._1
          val imei = row._5
          val parentgameid = jedis0.hget(row._3.toString + "_publish_game", "mainid")
          val parent_game_id = if (parentgameid == null || parentgameid.equals("")) "0" else parentgameid
          val publish_time = row._2.substring(0, 13)
          val publish_date = row._2.substring(0, 10)
          val pkgid = row._4
          val medium_channel = StringUtils.getArrayChannel(row._4)(0)
          val ad_site_channel = StringUtils.getArrayChannel(row._4)(1)
          val pkg_code = StringUtils.getArrayChannel(row._4)(2)
          var medium_account = jedis0.hget(pkg_code + "_pkgcode", "medium_account")
          if (medium_account == null) medium_account = ""
          var promotion_channel = jedis0.hget(pkg_code + "_pkgcode", "promotion_channel")
          if (promotion_channel == null) promotion_channel = ""
          var promotion_mode = jedis0.hget(pkg_code + "_" + publish_date + "_pkgcode", "promotion_mode")
          if (promotion_mode == null) promotion_mode = ""
          var head_people = jedis0.hget(pkg_code + "_" + publish_date + "_pkgcode", "head_people")
          if (head_people == null) head_people = ""
          var os = Commons.getPubGameGroupIdAndOs(game_id, connFx)(1)
          if (os == null) {
            println("loginActions get os is err:" + os);
            os = "1"
          }
          var group_id_pre = Commons.getPubGameGroupIdAndOs(game_id, connFx)(0)
          if (group_id_pre == null) {
            println("loginActions groupid os is err:" + group_id_pre);
            group_id_pre = "0"
          }
          val group_id = group_id_pre.toInt
          //通过游戏帐号获取游戏帐号信息
          val accountInfo = jedis0.hgetAll(game_account)

          //发行二期使用指标

          //是否为新增注册设备
          val isNewRegiDevDay = GamePublicDao.isNewRegiDevDay(pkg_code, imei, publish_date, medium_channel, ad_site_channel, game_id, conn)
          //是否为当日注册帐号，并且与注册设备的imei，推广渠道一致
          val isNewRegiAccountDay = GamePublicDao.isNewRegiAccountDay(game_account, publish_date, isNewRegiDevDay, pkgid, imei, jedis0, accountInfo)


        }
      }
    }

  }


}
