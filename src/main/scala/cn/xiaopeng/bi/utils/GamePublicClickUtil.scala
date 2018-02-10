package cn.xiaopeng.bi.utils

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bigdata on 17-11-1.
  */
object GamePublicClickUtil {
  val logger = Logger.getLogger(GamePublicClickUtil.getClass)
  var arg = "60"

  def loadClickInfo(rdd: RDD[String], hiveContext: HiveContext): Unit = {
    StreamingUtils.parseChannelToTmpTable(rdd, hiveContext)
    StreamingUtils.parseRequestToTmpTable(rdd, hiveContext)

    val clickDF = hiveContext.sql("select distinct click.* from (select * from request union select * from channel) click join lastPubGame pg on click.game_id = pg.game_id ")
    clickDF.foreachPartition(rows => {
      //创建redis客户端
      val pool = JedisUtil.getJedisPool
      val jedis = pool.getResource

      val conn = JdbcUtil.getConn()
      val statement = conn.createStatement()

      val sqlText = "insert into bi_gamepublic_base_day_kpi(publish_date,child_game_id,medium_channel," +
        "ad_site_channel,pkg_code,adpage_click_uv,request_click_uv,show_uv,download_uv," +
        "parent_game_id,os,medium_account,promotion_channel,promotion_mode,head_people,group_id)" +
        " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)" +
        " on duplicate key update adpage_click_uv=adpage_click_uv+?,request_click_uv=request_click_uv+?,show_uv=show_uv+?,download_uv=download_uv+?"
      val sqlDetailText = "insert into bi_gamepublic_click_detail(publish_date,game_id,medium_channel,ad_site_channel,pkg_code,type,ip,create_time) values(?,?,?,?,?,?,?,?)"
      val params = new ArrayBuffer[Array[Any]]()
      val detailParams = new ArrayBuffer[Array[Any]]()

      rows.foreach(row => {
        val channelArray = StringUtils.getArrayChannel(row.get(0).toString)
        val querySql = "select count(1) from bi_gamepublic_click_detail where publish_date ='" + row.getString(2) + "' and game_id = " + row.getInt(1) + "" +
          " and type = " + row.getInt(3) + " and ip='" + row.getString(4) + "'" +
          " and medium_channel = '" + channelArray(0) + "' and ad_site_channel = '" + channelArray(1) + "' and pkg_code='" + channelArray(2) + "'"
        val rs = statement.executeQuery(querySql)
        if (rs.next()) {
          val channelArray = StringUtils.getArrayChannel(row.get(0).toString)
          val game_id = row.getInt(1)
          val pkg_code = channelArray(2)
          val order_date = row.getString(2)
          val redisValue = JedisUtil.getRedisValue(game_id, pkg_code, order_date, jedis)
          //1:展示数 2：下载数 3：广告展示 4：活动页
          val typeId = row.getInt(3)
          val typeValue = StreamingUtils.getValueByClickType(typeId)

          if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 15) {
            params.+=(Array[Any](row.get(2), row.get(1), channelArray(0), channelArray(1), channelArray(2)
              , typeValue(0), typeValue(1), typeValue(2), typeValue(3),
              redisValue(0), redisValue(1), redisValue(2), redisValue(3), redisValue(4), redisValue(5), redisValue(6)
              , typeValue(0), typeValue(1), typeValue(2), typeValue(3)))

            detailParams.+=(Array[Any](row.get(2), row.get(1), channelArray(0), channelArray(1), channelArray(2), row.get(3), row.get(4), DateUtils.getTodayTime()))
          }

        }
      })

      try {
        JdbcUtil.doBatch(sqlDetailText, detailParams, conn)
        JdbcUtil.doBatch(sqlText, params, conn)
      } catch {
        case e: Exception => {
          logger.error("============插入click表异常：" + e)
        }
      } finally {
        statement.close()
        conn.close()
      }
      pool.returnResource(jedis)
      pool.destroy()


    })

  }
}
