package cn.xiaopeng.bi.utils

import java.sql.{Connection, PreparedStatement}

import redis.clients.jedis.Jedis

/**
  * Created by bigdata on 7/18/17.
  */
object GamePublicDao {

  /**
    * 从库中判断是否为新注册设备
    *
    * @param pkgCode
    * @return
    */
  def isNewRegiDevDay(pkgCode:String,imei:String,publishDate:String,medium_channel:String,ad_site_channel:String,gameId:Int,conn:Connection) :Int={
    var res = 0
    val sql2Mysql = "select left(regi_hour,10) as publish_date from bi_gamepublic_regi_detail " +
      "where ad_label=? and imei=? and parent_channel=? and child_channel=? and game_id=? order by regi_hour asc limit 1"
    val ps:PreparedStatement = conn.prepareStatement(sql2Mysql)
    ps.setString(1,pkgCode)
    ps.setString(2,imei)
    ps.setString(3,medium_channel)
    ps.setString(4,ad_site_channel)
    ps.setInt(5,gameId)
    val result = ps.executeQuery()
    while(result.next()){
      if(result.getString("publish_date").equals(publishDate)){
        res = 1
      }else res =0
    }
    ps.close()
    return res
  }
  /**
    * 是否当日注册账号
    *
    * @param gameAccount
    * @return
    */
  def isNewRegiAccountDay(gameAccount:String,regiDate:String,isNewRegiDay:Int,pkgId:String,Imei:String,jedis:Jedis):Int={
    var res = 0
    jedis.select(0)
    val arr = jedis.hgetAll(gameAccount)
    var reg_time = arr.get("reg_time")
    val expandChannel = arr.get("expand_channel")
    val imei = arr.get("imei")
    if(reg_time == null){
//      val s = MissInfo2Redis.checkAccount(gameAccount)

    }
    1
  }



}
