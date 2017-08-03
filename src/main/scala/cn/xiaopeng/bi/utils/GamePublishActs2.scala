package cn.xiaopeng.bi.utils

import java.sql.Connection

import org.apache.spark.rdd.RDD
import redis.clients.jedis.{Jedis, JedisPool}

/**
  * Created by bigdata on 17-8-2.
  */
object GamePublishActs2 {


  def loadLoginInfo(dslogs: RDD[String]) = {
    val rdd = dslogs.filter(x => {
      val lgInfo = x.split("\\|")
      lgInfo(0).contains("bi_login") && lgInfo.length >= 8
    }).map(x => {
      val lgInfo = x.split("\\|")
      //game_account(3),logintime(4),game_id(8)，expand_channel(6),imei(7).sub
      (lgInfo(3).trim().toLowerCase(), lgInfo(4), lgInfo(8).toInt, if (lgInfo(6).equals("") || lgInfo(6) == null) "21" else lgInfo(6), lgInfo(7))
    })
    rdd.foreachPartition(fp => {
      loginActions(fp)
    })
    //关闭
    JedisPoolSingleton.destroy()
  }

  /**
    * 用户登录行为
    *
    * @param fp
    */
  def loginActions(fp: Iterator[(String, String, Int, String, String)]): Unit = {
    val pool: JedisPool = JedisPoolSingleton.getJedisPool()
    val jedis0: Jedis = pool.getResource
    jedis0.select(0) //选0号库
    val jedis3: Jedis = pool.getResource
    jedis3.select(3) //选3号库
    val conn: Connection = JdbcUtil.getConn()
    val connFx: Connection = JdbcUtil.getXiaopeng2FXConn()
    for (row <- fp) {
      //若已经同一日志被统计一遍，则不许要再统计
      if (!jedis3.exists(row._1 + "|" + row._2 + "|" + "_publicLoginActions")) {
        //判断是否为发行游戏，只取发行游戏
        if (jedis0.exists(row._3.toString + "_publish_game")) {
          val game_id = row._3.toInt
          val game_account = row._1
          val imei = row._5
          val parentgameid = jedis0.hget(row._3.toString + "_publish_game", "mainid")
          val parent_game_id = if (parentgameid == null || parentgameid.equals("")) "" else parentgameid
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
          val os = Commons.getPubGameGroupIdAndOs(game_id, connFx)(1)
          val group_id = Commons.getPubGameGroupIdAndOs(game_id, connFx)(0).toInt
          //发行二期使用指标

          //是否为新增注册设备
          val isNewRegiDevDay = GamePublicDao.isNewRegiDevDay(pkg_code, imei, publish_date, medium_channel, ad_site_channel, game_id, conn)
          //是否为当日注册帐号，并且与注册设备的imei、推广渠道一致
          val isNewRegiAccountDay = GamePublicDao.isNewRegiAccountDay(game_account, publish_date, isNewRegiDevDay, pkgid, imei, jedis0)
          //是否新登录设备、新注册设备并且新注册帐号，并且一天只算一次
          val isNewLgDev = GamePublicDao.isNewLgDevDay(pkgid, imei, publish_date, game_id, isNewRegiDevDay, isNewRegiAccountDay, jedis3)
          //是否为新增登录帐号，新注册设备并且新注册帐号，并且一天只算一次
          val isNewLgAccount = GamePublicDao.isNewLgAccountDay(pkgid, game_account, publish_date, game_id, isNewRegiDevDay, isNewRegiAccountDay, jedis3)
          //判断是否为新增活跃设备，新增设备大于2次登录，是则1
          val isNewActiveDev = GamePublicDao.isNewActiveDevDay(pkgid, imei, publish_date, game_id, jedis3)
          //是否设备当天已经登录，一天只算一次
          val isLoginDevDay = GamePublicDao.isLoginDevDay(pkgid, imei, publish_date, game_id, jedis3)
          //是否帐号当天已经登录，一天只算一次
          val isLoginAccountDay = GamePublicDao.isLoginAccountDay(pkgid, game_account, publish_date, game_id, jedis3)
          //是否帐号当天该小时已经登录，一天只算一次
          val isLoginAccountHour = GamePublicDao.isLoginAccountHour(pkgid, game_account, publish_time, game_id, jedis3)
          //设置data to  tupel19  按日期
          val tp19 = new Tuple19(publish_date, parent_game_id, game_id, medium_channel, ad_site_channel, pkg_code,
            medium_account, promotion_channel, promotion_mode, head_people, os, group_id,
            isNewLgDev, isNewLgAccount, isNewActiveDev, isLoginDevDay, isLoginAccountDay, imei, game_account)
          //小时表使用指标
          val tu14 = new Tuple14(publish_time, parent_game_id, game_id, medium_channel, ad_site_channel, pkg_code, medium_account, promotion_channel
            , promotion_mode, head_people, os, group_id, isLoginAccountHour, game_account)

          //---发行三期数据部分指标---

          //是否为新增注册设备
          val isNewRegiDevDay2 = GamePublicDao2.isNewRegiDevDay(imei, publish_date,  game_id, conn)
          //是否为当日注册帐号，并且与注册的渠道一致
          val isNewRegiAccountDay2 = GamePublicDao2.isNewRegiAccountDay(game_account, publish_date, isNewRegiDevDay2,imei, jedis0)
          //判断是否设备有记录，若有记录则为0，否则为1
          val isNewLgDev2 = GamePublicDao2.isNewLgDevDay(imei, publish_date,game_id, isNewRegiDevDay2,isNewRegiAccountDay2, jedis3)
          val isNewLgAccount2 = GamePublicDao2.isNewLgAccountDay(game_account, publish_date,game_id, isNewRegiDevDay2, isNewRegiAccountDay2, jedis3)
          //是否设备当天已经登录，若已经登录为0
          val isLoginDevDay2 = GamePublicDao2.isLoginDevDay(imei, publish_date,game_id, jedis3)
          //设置data to tupel16  按日期
          val tp10 = new Tuple10(publish_date, parent_game_id, game_id,os, group_id,
            isNewLgDev2, isNewLgAccount2, isLoginDevDay2, imei, game_account)

          //调用函数对数据进行加载
          try {
            GamePublicDao.loginActionsByDayProcessDB(tp19, conn, pkgid)//发行二期基础表
            GamePublicDao.loginAccountByDayProcessDB(tp19,conn)  //日活跃
            GamePublicDao.loginAccountDauByHourProcessDB(tu14,conn,pkgid)//小时表
            GamePublicDao2.loginActionsByDayProcessDB(tp10,conn) //发行三期基础表
            //缓存一下数据，避免重复计算
            //对本小时第一次登录的记录到redis
            jedis3.set("isLoginAccountHour|" + tu14._1 + "|" + tu14._14 + "|" + pkgid+"|"+tu14._3.toString, tu14._6)
            jedis3.expire("isLoginAccountHour|" + tu14._1 + "|" + tu14._14 + "|" + pkgid+"|"+tu14._3.toString, 3600 * 8)

          } catch {
            case e:Exception => e.printStackTrace()
          }

        }
      }
    }
  }
}
