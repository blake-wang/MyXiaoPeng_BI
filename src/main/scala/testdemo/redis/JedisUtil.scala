package testdemo.redis

import cn.wanglei.bi.{ConfigurationUtil, Constants}
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
  * Created by bigdata on 17-8-4.
  */
object JedisUtil {
  def getJedisPool: JedisPool = {
    val host = "192.168.20.177"
    val port = 6379
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
    new JedisPool(poolConfig, host, port, 10000, "redis")
  }

}
