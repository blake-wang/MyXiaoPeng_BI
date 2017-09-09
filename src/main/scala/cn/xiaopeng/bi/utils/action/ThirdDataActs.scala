package cn.xiaopeng.bi.utils.action

import java.text.SimpleDateFormat
import java.util.Date

import cn.xiaopeng.bi.utils.dao.ThirdDataDao
import cn.xiaopeng.bi.utils.{CommonsThirdData, JdbcUtil, JedisUtil, StringUtils}
import net.sf.json.JSONObject
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.JedisPool

/**
  * Created by wanglei on 2017/9/8.
  */
object ThirdDataActs {

  //激活匹配点击数据
  def activeMatchClick(dataActive: DStream[(String, String, String, String, String, String)]) = {
    dataActive.foreachRDD(rdd => {
      rdd.cache()
      rdd.foreachPartition(fp => {
        val pool: JedisPool = JedisUtil.getJedisPool
        val jedis = pool.getResource
        val jedis6 = pool.getResource
        jedis6.select(6)
        val conn = JdbcUtil.getConn()
        val connFx = JdbcUtil.getXiaopeng2FXConn()
        fp.foreach(line => {
          val gameId = line._1.toInt
          //这一步非常重要，有广告投放的游戏才统计
          if (CommonsThirdData.isNeedStaGameId(gameId, conn)) {
            //这里的imei是从激活日志中取出的
            //安卓设备取&最前面部分，苹果设备直接是32位idfa
            var imei = CommonsThirdData.getImei(line._4)
            //取出分包id                                           --这里只有android设备才匹配这个，设备匹配这个没有意义
            var pkgCode = StringUtils.getArrayChannel(line._3)(2) //取出这个，只是为了匹配按着设备
            //取出激活时间 2017-09-09 00:00:00
            val dt = line._5
            val os = if (line._6.toLowerCase.equals("ios")) {
              2
            } else {
              1
            }
            //激活时间
            val activeTime = dt
            //激活日期
            val activeDate = dt.substring(0, 10)
            //激活时间匹配点击时间  往前推7天
            val dt7before = CommonsThirdData.getDt7Before(dt, -7)
            var matched = Tuple8(0, "", "", "", 0, "", "", "")
            //os=2 是 ios， os=1是android
            if (os == 2) {
              //再过滤一次有效设备，过滤掉imei不合格的日志
              if (CommonsThirdData.isVadDev(imei, 2, 2)) {
                //ios
                //查询数据库之前，要把ios的idfs转换成带横杠的
                imei = CommonsThirdData.getYSIdfa(imei.toUpperCase())
                //什么是自由平台？
                //匹配值 0-未匹配，匹配不成功，算到自由平台，imei-原生idfa(包括-)，我们平台的imei
                matched = ThirdDataDao.matchClickIos(imei, dt7before, dt, gameId, conn) //苹果设备的匹配是通过imei和Id 来匹配的
              }
            } else if (os == 1) {
              //android
              matched = ThirdDataDao.matchClickAndroid(pkgCode, imei, dt7before, dt, conn) //苹果设备的匹配是通过pkgCode和imei 来匹配的
            }
            //如果匹配成功，则要计算设备统计数据，并且写入激活明细表
            if (matched._1 >= 1) {
              val redisValue: Array[String] = CommonsThirdData.getRedisValue(gameId, pkgCode, activeDate, jedis, connFx)
              val medium = matched._5
              val group_id = redisValue.apply(6)
              val medium_account = redisValue(2)
              val head_people = redisValue(5)
              val idea_id = matched._6
              val first_level = matched._7
              val second_level = matched._8
              val activeNum = 1
              //苹果设备，匹配点击日志中的pkgCode，安卓设备，匹配我们自己的激活日志中的expand_channel第三部分
              pkgCode = if (os == 2) matched._2 else pkgCode
              imei = matched._4
              //激活统计 激活数
              ThirdDataDao.insertActiveStat(activeDate, gameId, group_id, pkgCode, head_people, medium_account, medium, idea_id, first_level, second_level, activeNum, conn)
              //把激活匹配数据写入到明细，后期注册统计使用
              if (matched._1 >= 1) {
                //匹配成功
                ThirdDataDao.insertActiveDetail(activeTime, imei, pkgCode, medium, gameId, os, conn)
              }
            }
          }
        })
        pool.returnResource(jedis)
        pool.returnResource(jedis6)
        pool.destroy()
        conn.close()
        connFx.close()

      })
      rdd.unpersist()
    })
  }

  //注册匹配激活
  def regiMatchActive(dataRegi: DStream[(String, Int, String, String, String, String)]) = {
    dataRegi.foreachRDD(rdd => {
      rdd.cache()
      rdd.foreachPartition(part => {
        val pool: JedisPool = JedisUtil.getJedisPool;
        val jedis = pool.getResource
        jedis.select(0)
        val jedis6 = pool.getResource
        jedis6.select(6)
        val conn = JdbcUtil.getConn()
        val connFx = JdbcUtil.getXiaopeng2FXConn()
        part.foreach(line => {
          val gameId = line._2.toInt
          if (CommonsThirdData.isNeedStaGameId(gameId, conn)) {
            val imei = CommonsThirdData.getImei(line._5)
            var pkgCode = line._3
            val gameAccount = line._1
            val os = if (line._6.equals("android")) 1 else 2
            if (CommonsThirdData.isVadDev(imei, 2, 2)) {
              val regiTime = line._4
              val regiDate = line._4.substring(0, 10)
              val dt30before = CommonsThirdData.getDt7Before(regiDate, -30)
              val matched: (Int, String, String, String, String) = CommonsThirdData.regiMatchActive(pkgCode, imei, dt30before, regiTime, gameId, os, conn)
              pkgCode = if (matched._1 >= 1) {
                //如果adName >= 1，说明是第三方广告平台
                matched._5
              } else if (os == 2) {
                //adName=0 并且 os=2
                ""
              } else {
                pkgCode
              }
              //adName如果大于1，说明匹配到了广告平台
              //如果匹配到了，则要计算注册统计数据，并且写入注册明细表，计算消费匹配时使用
              if (matched._1 >= 1) {
                val redisValue: Array[String] = CommonsThirdData.getRedisValue(gameId, pkgCode, regiDate, jedis, connFx)
                val medium = matched._1
                val group_id = redisValue(6)
                val medium_account = redisValue(2)
                val head_people = redisValue(5)
                val idea_id = matched._2
                val first_level = matched._3
                val second_level=matched._4
                val regiNum =1
                val topic = medium match{
                  case 1 => "momo"
                  case 2 => "baidu"
                  case 3 => "jinritoutiao"
                  case 5 => "uc"
                  case 4 => "aiqiyi"
                  case 6 => "guangdiantong"
                  case 0 => "pyw"
                }
                //在redis中做匹配 ，匹配维度为  topic+pkgCode+imei+regiDate
                val regiDev = CommonsThirdData.isRegiDev(regiDate,pkgCode,imei,topic,jedis6)

              }
            }
          }
        })

      })
    })
  }

  //处理公司自己的日志 ,日志包括  激活，注册，订单
  def theOwnerData(ownerData: DStream[String]) = {
    //激活匹配点击
    val dataActive = ownerData.filter(ats => {
      ats.contains("bi_active")
    }).map(actives => {
      val splitd = actives.split("\\|", -1)
      //gameid,channelid,expand_channel,imei,date,os
      (splitd(1), splitd(2), splitd(4), splitd(8), splitd(5), splitd(7))

    })
    activeMatchClick(dataActive)

    //注册匹配点击
    val dataRegi = ownerData.filter(ats => ats.contains("bi_regi")).filter(line => {
      val rgInfo = line.split("\\|", -1)
      rgInfo.length > 14 && StringUtils.isNumber(rgInfo(4))
    }).map(regi => {
      val arr = regi.split("\\|", -1)
      //game_account,game_id,expand_channel(渠道用_分割，取第三段 分包id),reg_time,imei,os
      (arr(3), arr(4).toInt, StringUtils.getArrayChannel(arr(13))(2), arr(5), arr(14), arr(11).toLowerCase())
    })
    regiMatchActive(dataRegi)

  }


  //处理第三方点击 --把所有的第三方日志整合到一起了，都用9个字段的元组 包裹起来，不用管是哪个平台，只是从每一行数据去取值
  def theThirdData(tdata: DStream[(String, String, String, String, String, Int, String, String, String)]) = {
    tdata.foreachRDD(rdd => {
      //显式调用cache方法，缓存rdd
      rdd.cache()
      rdd.foreachPartition(part => {
        val conn = JdbcUtil.getConn()
        val pool = JedisUtil.getJedisPool
        val jedis = pool.getResource
        val jedis6 = pool.getResource
        //选择redis6号库
        jedis6.select(6)
        val connFx = JdbcUtil.getXiaopeng2FXConn()
        part.foreach(line => {
          //这里做这个判断，是为了排除日志被截断，不完整，等一场情况，json解析过程中，发生异常，默认给的是0
          if (line._4 != 0) {
            //陌末原始日志中的os ，1是android，2是ios
            val os = line._2
            val osInt = if (os.toLowerCase.equals("ios")) 2 else 1
            val imei = line._3
            //这个advName是丽辉在数据表中定义的  1是默默 ，2是百度
            val advName = line._6.toInt
            //这个判断是为了过滤imei不合格的日志
            if (CommonsThirdData.isVadDev(imei, osInt, advName)) {
              //这个时间是已经转换后的时间 格式为 2017-09-07 00:00:00
              val ts = line._4
              //取出的是点击日期
              val clickDate = ts.substring(0, 10)
              //原始日志中的pkgCode 为4151M19006  ,M 以前的4151才是gameId
              val pkgCode = line._1
              val gameId = pkgCode.substring(0, pkgCode.indexOf("M")).toInt
              val url = line._5


              //点击日志的匹配通过 pkgCode,imei,matched=0  三个指标来匹配
              //这个点击记录是，如果能匹配到记录，就说明已经存在，如果匹配不到，就存入数据库
              //点击是否已经被记录，若被记录，更新 字段 ts,callbackurl
              val isExistClick = CommonsThirdData.isExistClick(pkgCode, imei, url, conn)
              //---1---<现在这里更新的日志数据都是在数据表bi_ad_momo_click，目前测试的都基本是默默的数据>   这里后续还要加入其他广告平台
              //等于1是匹配到了，不等于1，就是没有匹配到
              if (isExistClick == 1) {
                //如果点击已经存在，就更新ts和url
                ThirdDataDao.updateMomoClickTs(pkgCode, imei, ts, url, conn)
              } else {
                //如果点击不存在，就插入新书据到明细表
                ThirdDataDao.insertMomoClick(pkgCode, imei, ts, os, url, gameId.toString, advName, conn)
              }


              //---2---<这里更新的日志数据都是插入数据表bi_ad_channel_stats>

              //parent_game_id,os,medium_account,promotion_channel,promotion_mode,head_people,group_id
              val redisValue: Array[String] = CommonsThirdData.getRedisValue(gameId, pkgCode, clickDate, jedis, connFx)
              //点击数 1
              val clicks = 1
              val topic = advName match {
                case 0 => "pyw"
                case 1 => "momo"
                case 2 => "baidu"
                case 3 => "jinritoutiao"
                case 5 => "uc"
                case 4 => "aiqiyi"
                case 6 => "guangdiantong"
              }
              //clickDevs 的数值是 0 或者 1
              val clickDevs = CommonsThirdData.isClickDev(clickDate, pkgCode, imei, topic, jedis6)
              val medium = advName
              val group_id = redisValue.apply(6)
              val medium_account = redisValue(2)
              val head_people = redisValue(5)
              val idea_id = line._7
              val first_level = line._8
              val second_level = line._9
              //插入点击信息统计表bi_ad_channel_stats   ,这个表的作用是用来统计点击信息
              //点击数 clicks 默认就是1，来一条日志，点击数就+1
              //点击设备数 clickDevs ，要按设备去重，如果设备不重复，就+1，设备重复，就+0
              ThirdDataDao.insertClickStat(clickDate, gameId, group_id, pkgCode,
                head_people, medium_account, medium, idea_id, first_level, second_level, clicks, clickDevs, conn)
            }
          }
        })
        pool.returnBrokenResource(jedis)
        pool.returnBrokenResource(jedis6)
        pool.destroy()
        conn.close()
        connFx.close()
      })

      //显式调用unpersist方法，移除缓存数据
      rdd.unpersist()
    })

  }

  def adClick(thirdData: DStream[String]) = {
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val tdata = thirdData.filter(x => x.contains("bi_adv_momo_click")).map(line => {
      try {
        val jsStr = JSONObject.fromObject(line)
        (
          jsStr.get("pkg_id").toString,
          if (jsStr.get("os").toString.equals("2")) "android" else if (jsStr.get("os").toString.equals("1")) "ios" else "wp",
          if (jsStr.get("imei").toString.equals("")) jsStr.get("idfa").toString else jsStr.get("imei").toString,
          simpleDateFormat.format(new Date(jsStr.get("ts").toString.toDouble.toLong * 1000)),
          jsStr.get("callback").toString,
          1, "", "", ""
        )
      } catch {
        case e: Exception => e.printStackTrace;
          ("", "", "", "0", "", 1, "", "", "") //1是momo
      }
    }).union(
      thirdData.filter(x => x.contains("bi_adv_baidu_click")).map(line => {
        try {
          val jsStr = JSONObject.fromObject(line)
          (jsStr.get("pkg_id").toString,
            if (jsStr.get("os").toString.equals("1")) "android" else if (jsStr.get("os").toString.equals("2")) "ios" else "wp",
            jsStr.get("imei").toString,
            simpleDateFormat.format(new Date(jsStr.get("ts").toString.toDouble.toLong * 1000)),
            jsStr.get("callback_url").toString,
            2, jsStr.get("aid").toString, jsStr.get("uid").toString, jsStr.get("pid").toString //广告主标示 创意ID 广告第一层 广告第二次
          )
        } catch {
          case e: Exception => e.printStackTrace; ("", "", "", "0", "", 2, "", "", "")
        } //2是百度
      }).union(
        thirdData.filter(x => x.contains("bi_adv_jinretoutiao_click")).map(line => {
          try {
            val jsStr = JSONObject.fromObject(line)
            (jsStr.get("pkg_id").toString,
              if (jsStr.get("os").toString.equals("0")) "android" else if (jsStr.get("os").toString.equals("1")) "ios" else "wp",
              jsStr.get("imei").toString,
              simpleDateFormat.format(new Date(jsStr.get("timestamp").toString.toDouble.toLong * 1000)),
              jsStr.get("callback").toString,
              3, jsStr.get("adid").toString, "", jsStr.get("cid").toString //广告主标示 创意ID 广告第一层 广告第二次
            )
          } catch {
            case e: Exception => e.printStackTrace; ("", "", "", "0", "", 3, "", "", "")
          } //3是今日头条

        })
      )
    )

    theThirdData(tdata)
  }


}
