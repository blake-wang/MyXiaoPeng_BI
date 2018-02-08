package cn.xiaopeng.bi.utils

import java.sql.Connection

import cn.wanglei.bi.{ConfigurationUtil, Constants}
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}

/**
  * Created by bigdata on 7/18/17.
  */
object JedisUtil {


  /**
    * 获取redis连接池
    *
    * @return
    */
  def getJedisPool: JedisPool = {
    val host = ConfigurationUtil.getProperty(Constants.REDIS_HOST)
    val port = Integer.parseInt(ConfigurationUtil.getProperty(Constants.REDIS_PORTY))
    val poolConfig = new GenericObjectPoolConfig
    //设置最大空闲连接数
    poolConfig.setMaxIdle(100)
    //连接池的最大连接数
    poolConfig.setMaxTotal(100)
    //设置获取连接的最大等待时间
    poolConfig.setMaxWaitMillis(2000)
    //从连接池中获取连接的时候是否需要校验，这样可以保证取出的连接都是可用的
    poolConfig.setTestOnBorrow(true)
    new JedisPool(poolConfig, host, port, 30000, "redis")
  }

  /**
    * get 媒介帐号，推广渠道，推广模式，负责人，发行组.....info
    * parent_game_id,medium_account,promotion_channel,promotion_mode,head_people,os,groupid
    *
    */
  def getRedisValue(game_id: Int, pkg_code: String, order_date: String, jedis: Jedis) = {
    var parent_game_id = jedis.hget(game_id.toString + "_publish_game", "mainid")
    if (parent_game_id == null) parent_game_id = "0"
    var medium_account = jedis.hget(pkg_code + "_pkgcode", "medium_account")
    if (medium_account == null) medium_account = ""
    var promotion_channel = jedis.hget(pkg_code + "_pkgcode", "promotion_channel")
    if (promotion_channel == null) promotion_channel = ""
    var promotion_mode = jedis.hget(pkg_code + "_" + order_date + "_pkgcode", "promotion_mode")
    if (promotion_mode == null) promotion_mode = ""
    var head_people = jedis.hget(pkg_code + "_" + order_date + "_pkgcode", "head_people")
    if (head_people == null) head_people = ""
    var os = jedis.hget(game_id.toString + "_publish_game", "system_type")
    if (os == null) {
      println(game_id + ":getRedisValue get os is err:" + os)
      os = "1"
    }
    var groupid = jedis.hget(game_id + "_publish_game", "publish_group_id")
    if (groupid == null) {
      println(game_id + ":getRedisValue get groupid is err:" + groupid);
      groupid = "0"
    }

    Array[String](parent_game_id, os, medium_account, promotion_channel, promotion_mode, head_people, groupid)

  }

}
