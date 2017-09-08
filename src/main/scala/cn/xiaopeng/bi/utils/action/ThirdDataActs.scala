package cn.xiaopeng.bi.utils.action

import java.text.SimpleDateFormat
import java.util.Date

import cn.xiaopeng.bi.utils.dao.ThirdDataDao
import cn.xiaopeng.bi.utils.{CommonsThirdData, JdbcUtil, JedisUtil}
import net.sf.json.JSONObject
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by wanglei on 2017/9/8.
  */
object ThirdDataActs {


  //处理第三方点击
  def theThirdData(tdata: DStream[(String, String, String, String, String, Int, String, String, String)]) = {
    tdata.foreachRDD(rdd => {
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
          if (line._4 != 0) {
            val ts = line._4
            val clickDate = ts.substring(0, 10)
            val os = line._2
            val pkgCode = line._1
            val gameId = pkgCode.substring(0, pkgCode.indexOf("M")).toInt
            val imei = line._3
            val url = line._5
            val advName = line._6.toInt //广告主ID
            //点击是否已经被记录，若被记录，更新 字段 ts,callbackurl
            val isExistClick = CommonsThirdData.isExistClick(pkgCode, imei, url, conn)
            if (isExistClick == 1) {
              //如果点击已经存在，就更新ts和url
              ThirdDataDao.updateMomoClickTs(pkgCode, imei, ts, url, conn)
            } else {
              //如果点击不存在，就插入新书据到明细表
              ThirdDataDao.insertMomoClick(pkgCode, imei, ts, os, url, gameId.toString, advName, conn)
            }

            //parent_game_id,os,medium_account,promotion_channel,promotion_mode,head_people,group_id
            val redisValue: Array[String] = CommonsThirdData.getRedisValue(gameId, pkgCode, clickDate, jedis, connFx)
            val clicks = 1 //点击数 1
            val topic = advName match {
              case 1 => "momo"
              case 2 => "baidu"
              case 3 => "jinritoutiao"
              case 5 => "uc"
              case 4 => "aiqiyi"
              case 6 => "guangdiantong"
              case 0 => "pyw"
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
            //插入点击统计信息到统计表
            ThirdDataDao.insertClickStat(clickDate,gameId,group_id,pkgCode,
              head_people,medium_account,medium,idea_id,first_level,second_level,clicks,clickDevs,conn)


          }
        })
      })
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
