package testdemo.redis

import redis.clients.jedis.Jedis

/**
  * Created by bigdata on 17-8-4.
  */
object MapMainDemo {
  def main(args: Array[String]): Unit = {
    val pool = JedisUtil.getJedisPool
    val jedis = pool.getResource
    val jedis2 = pool.getResource
    jedis.select(1)
    jedis2.select(10)
//    hSet(jedis)
    hGet(jedis)
    hGet(jedis2)

  }

  def hGet(jedis:Jedis): Unit ={
    val value = jedis.hget("daxia:jingzhongyue","姓名")
    println(value)
  }

  def hSet(jedis: Jedis): Unit = {
    jedis.hset("daxia:jingzhongyue","姓名","不为人知")
  }

}
