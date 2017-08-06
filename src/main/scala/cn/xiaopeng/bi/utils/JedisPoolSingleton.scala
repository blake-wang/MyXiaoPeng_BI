package cn.xiaopeng.bi.utils

import cn.wanglei.bi.{ConfigurationUtil, Constants}
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}

/**
  * Created by bigdata on 7/18/17.
  */
object JedisPoolSingleton {
  private var instance: JedisPool = _

  def getJedisPool(): JedisPool = {
    if (instance == null) {
      val host = ConfigurationUtil.getProperty(Constants.REDIS_HOST)
      val port = new Integer(ConfigurationUtil.getProperty(Constants.REDIS_PORTY))
      val poolConfig = new GenericObjectPoolConfig
      //最大空闲连接数
      poolConfig.setMaxIdle(100)
      //连接池的最大连接数
      poolConfig.setMaxTotal(10000)
      //设置获取连接的最大等待时间
      poolConfig.setMaxWaitMillis(9000)
      //从连接池中获取连接的时候是否需要校验，这样可以保证取出的连接都是可用的
      poolConfig.setTestOnBorrow(true)
      //获取jedis连接池
      instance = new JedisPool(poolConfig, host, port, 10000, "redis")
    }
    instance
  }

  def returnResource(jedis: Jedis) = {
    if (instance != null && jedis != null) {
      instance.returnResource(jedis)
    }
  }

  def destroy() = {
    if (instance != null) {
      instance.destroy()
      instance = null
    }
  }

}
