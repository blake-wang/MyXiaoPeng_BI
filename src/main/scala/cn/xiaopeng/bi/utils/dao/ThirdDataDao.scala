
package cn.xiaopeng.bi.utils.dao

import java.sql.{Connection, PreparedStatement}

import cn.wanglei.bi.utils.MD5Util


/**
  * Created by bigdata on 17-9-8.
  */
object ThirdDataDao {
  //把订单明细写入到订单详情表中
  def insertOrderDetail(orderTime: String, imei: String, pkgCode: String, medium: Int, gameId: Int, os: Int, gameAccount: String, payPrice: Float, conn: Connection) = {
    val orderSql = "insert into bi_ad_order_o_detail (pkg_id,game_id,imei,os,order_time,adv_name,game_account,pay_price) values (?,?,?,?,?,?,?,?) on duplicate key update order_time=?,pay_price=pay_price+?"
    val ps: PreparedStatement = conn.prepareStatement(orderSql)
    //insert
    ps.setString(1, pkgCode)
    ps.setInt(2, gameId)
    ps.setString(3, imei)
    ps.setInt(4, os)
    ps.setString(5, orderTime)
    ps.setInt(6, medium)
    ps.setString(7, gameAccount)
    ps.setFloat(8, payPrice)
    //update
    ps.setString(9, orderTime)
    ps.setFloat(10, payPrice)
    ps.executeUpdate()
    ps.close()
  }


  //把注册明细写入到注册明细表中
  def insertRegiDetail(regiTime: String, imei: String, pkgCode: String, medium: Int, gameId: Int, os: Int, gameAccount: String, conn: Connection) = {
    val instSql = "insert into bi_ad_regi_o_detail(pkg_id,game_id,imei,os,regi_time,adv_name,game_account) values(?,?,?,?,?,?,?) on duplicate key update regi_time=? "
    val ps: PreparedStatement = conn.prepareStatement(instSql)
    //insert
    ps.setString(1, pkgCode)
    ps.setInt(2, gameId)
    ps.setString(3, imei)
    ps.setInt(4, os)
    ps.setString(5, regiTime)
    ps.setInt(6, medium)
    ps.setString(7, gameAccount)
    //update
    ps.setString(8, regiTime)
    ps.executeUpdate()
    ps.close()
  }


  //注册帐号，注册设备数统计
  def insertRegiStat(regiDate: String, gameId: Int, group_id: String, pkgCode: String, head_people: String, medium_account: String, medium: Int, idea_id: String, first_level: String, second_level: String, regiNum: Int, regiDev: Int, conn: Connection) = {
    val sql2Mysql = "insert into bi_ad_channel_stats" +
      "(publish_date,game_id,group_id,pkg_id,head_people,medium_account,medium,idea_id,first_level,second_level,regi_num,regi_dev_num)" +
      " values(?,?,?,?,?,?,?,?,?,?,?,?) " +
      " on duplicate key update regi_num=regi_num+?,regi_dev_num=regi_dev_num+?,group_id=?,head_people=?,medium_account=?"
    val ps: PreparedStatement = conn.prepareStatement(sql2Mysql)
    //insert
    ps.setString(1, regiDate)
    ps.setInt(2, gameId)
    ps.setString(3, group_id)
    ps.setString(4, pkgCode)
    ps.setString(5, head_people)
    ps.setString(6, medium_account)
    ps.setInt(7, medium)
    ps.setString(8, idea_id)
    ps.setString(9, first_level)
    ps.setString(10, second_level)
    ps.setInt(11, regiNum)
    ps.setInt(12, regiDev)
  }


  //插入订单数据
  def insertOrderStat(orderDate: String, gameId: Int, group_id: String, pkgCode: String, head_people: String, medium_account: String, medium: Int, idea_id: String, first_level: String, second_level: String, payPrice: Float, payAccs: Int, newPayPrice: Float, newPayAccs: Int, conn: Connection) = {
    val sql2MySql = "insert into bi_ad_channel_stats" +
      "(publish_date,game_id,group_id,pkg_id,head_people,medium_account,medium,idea_id,first_level,second_level,pay_price,pay_accounts,new_pay_price,new_pay_accounts)" +
      " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)" +
      "on duplicate key update pay_price=pay_price+?,pay_accounts=pay_accounts+?,new_pay_price=new_pay_price+?,new_pay_accounts=new_pay_accounts+?"
    val ps: PreparedStatement = conn.prepareStatement(sql2MySql)
    //insert
    ps.setString(1, orderDate)
    ps.setInt(2, gameId)
    ps.setString(3, group_id)
    ps.setString(4, pkgCode)
    ps.setString(5, head_people)
    ps.setString(6, medium_account)
    ps.setInt(7, medium)
    ps.setString(8, idea_id)
    ps.setString(9, first_level)
    ps.setString(10, second_level)
    ps.setFloat(11, payPrice) //充值金额 ，单位到分
    ps.setInt(12, payAccs) //充值人数
    ps.setFloat(13, newPayPrice) //新增付费，单位到分
    ps.setInt(14, newPayAccs) //新增付费人数

    //update
    ps.setFloat(15, payPrice)
    ps.setInt(16, payAccs)
    ps.setFloat(17, newPayPrice)
    ps.setInt(18, newPayAccs)

    ps.executeUpdate()
    ps.close

  }


  //把激活明细写入到激活明细表中
  def insertActiveDetail(activeTime: String, imei: String, pkgCode: String, medium: Int, gameId: Int, os: Int, conn: Connection) = {
    val instSql = "insert into bi_ad_active_o_detail(pkg_id,game_id,imei,os,active_time,adv_name) values (?,?,?,?,?,?) on duplicate key update active_time=?"
    val ps: PreparedStatement = conn.prepareStatement(instSql)
    //insert
    ps.setString(1, pkgCode)
    ps.setInt(2, gameId)
    ps.setString(3, imei)
    ps.setInt(4, os)
    ps.setString(5, activeTime)
    ps.setInt(6, medium)
    //udpate
    ps.setString(7, activeTime)
    ps.executeUpdate()
    ps.close()
  }

  //广告渠道统计
  def insertActiveStat(activeDate: String, gameId: Int, group_id: String, pkgCode: String, head_people: String, medium_account: String, medium: Int, idea_id: String, first_level: String, second_level: String, activeNum: Int, conn: Connection) = {
    val sql2Mysql = "insert into bi_ad_channel_stats" +
      "(publish_date,game_id,group_id,pkg_id,head_people,medium_account,medium,idea_id,first_level,second_level,active_num)" +
      " values(?,?,?,?,?,?,?,?,?,?,?) " +
      " on duplicate key update active_num=active_num+?"
    val ps: PreparedStatement = conn.prepareStatement(sql2Mysql)
    /*insert*/
    ps.setString(1, activeDate)
    ps.setInt(2, gameId)
    ps.setString(3, group_id)
    ps.setString(4, pkgCode)
    ps.setString(5, head_people)
    ps.setString(6, medium_account)
    ps.setInt(7, medium)
    ps.setString(8, idea_id)
    ps.setString(9, first_level)
    ps.setString(10, second_level)
    ps.setInt(11, activeNum)

    /*update*/
    ps.setInt(12, activeNum)
    ps.executeUpdate()
    ps.close()

  }


  //对是否匹配进行更新-android
  def matchClickAndroid(pkgCode: String, imei1: String, dt7before: String, dt: String, conn: Connection): Tuple8[Int, String, String, String, Int, String, String, String] = {
    var imei = imei1
    var jg = 0
    var ps: PreparedStatement = null
    var pkg = pkgCode
    var tp5 = Tuple8(0, "", "", imei1, 0, "", "", "")
    //根据分包id来判断是哪个平台来的点击
    var advName = 0
    var ideaId = ""
    var firstLevel = ""
    var secondLevel = ""
    val adplafSql = "select adv_name from bi_ad_momo_click where pkg_id=? limit 1"
    ps = conn.prepareStatement(adplafSql)
    ps.setString(1, pkgCode)
    val adjg = ps.executeQuery()
    while (adjg.next()) {
      advName = adjg.getString("adv_name").toInt
    }
    //根据平台编号，判断是哪个平台
    //0是朋友玩，除了0，才是第三方平台
    if (advName != 0) {
      //1是陌陌，3是今日头条，需要加密
      //陌陌平台发的点击日志的android设备imei是加密的
      //所以，从我们自己的激活日志中拿出的imei需要md5加密后，再大写，才能和点击日志中的imei去匹配
      if (advName == 1 || advName == 3) {
        imei = MD5Util.md5(imei).toUpperCase()
      }
      //计算匹配
      //android设备的匹配，是通过 imei,pkg_id,ts和matched状态码
      val instSql = "update bi_ad_momo_click set matched=1 where imei=? and ts<=? and ts>=? and matched=0 and pkg_id=0 "
      ps = conn.prepareStatement(instSql)
      ps.setString(1, imei)
      ps.setString(2, dt)
      ps.setString(3, dt7before)
      ps.setString(4, pkgCode)
      jg = ps.executeUpdate()

      //根据匹配结果得出返回值
      if (jg > 0) {
        val sqSql = "select pkg_id,imei,idea_id,first_level,second_level from bi_momo_click where imei=? and ts<=? and ts>=? and matched=1 and pkg_id=? limit 1"
        ps = conn.prepareStatement(sqSql)
        ps.setString(1, imei)
        ps.setString(2, dt)
        ps.setString(3, dt7before)
        ps.setString(4, pkg)
        val rs = ps.executeQuery()
        while (rs.next()) {
          imei = rs.getString("imei")
          ideaId = rs.getString("idea_id")
          firstLevel = rs.getString("first_level")
          secondLevel = rs.getString("second_level")
        }
      }
    }
    ps.close()
    tp5 = new Tuple8(jg, pkg, imei, imei1, advName, ideaId, firstLevel, secondLevel)
    return tp5
  }


  //对是否匹配进行更新-ios
  def matchClickIos(idfa: String, dt7before: String, dt: String, gameId: Int, conn: Connection): Tuple8[Int, String, String, String, Int, String, String, String] = {
    var tp8 = Tuple8(0, "", "", "", 0, "", "", "")
    var pkg = ""
    val imei = idfa
    var adv_name = 0
    var ideaId = ""
    var firstLevel = ""
    var secondLevel = ""
    //苹果设备的匹配是 imei ,game_id,ts区间，matched状态码
    val instSql = "update bi_ad_momo_click set matched=1 where imei=? and ts<=? and ts >=? and matched=0 and game_id=? "
    var ps: PreparedStatement = conn.prepareStatement(instSql)
    ps.setString(1, imei)
    ps.setString(2, dt)
    ps.setString(3, dt7before)
    ps.setInt(4, gameId)
    //返回值是更新的行数
    val jg = ps.executeUpdate()
    //如果更新行数大于0，说明表中存在匹配数据
    if (jg > 0) {
      val slSql = "select pkg_id,adv_name,idea_id,first_level,second_level from bi_ad_momo_click where imei=? and ts<=? and ts>=? and matched=1 and game_id=? limit 1"
      ps = conn.prepareStatement(slSql)
      ps.setString(1, imei)
      ps.setString(2, dt)
      ps.setString(3, dt7before)
      ps.setInt(4, gameId)
      val rs = ps.executeQuery()
      while (rs.next()) {
        pkg = rs.getString("pkg_id")
        adv_name = rs.getInt("adv_name")
        ideaId = rs.getString("idea_id")
        firstLevel = rs.getString("first_level")
        secondLevel = rs.getString("second_level")
      }
    }
    ps.close()
    //bi_ad_momo_click表中的imei是带横杠的，在返回到外面的时候，把横杠去掉
    tp8 = new Tuple8(jg, pkg, imei, idfa.replace("-", ""), adv_name, ideaId, firstLevel, secondLevel)
    return tp8

  }

  //插入数据到bi_ad_channel_stats表，如果数据不存在就插入，如果存在，就更新点击数
  def insertClickStat(activeDate: String, gameId: Int, group_id: String, pkgCode: String, head_people: String, medium_account: String, medium: Int, idea_id: String, first_level: String, second_level: String, clicks: Int, clickDevs: Int, conn: Connection) = {
    val sql2Mysql = "insert into bi_ad_channel_stats" +
      "(publish_date,game_id,group_id,pkg_id,head_people,medium_account,medium,idea_id,first_level,second_level,click_num,click_dev_num)" +
      "values(?,?,?,?,?,?,?,?,?,?,?,?)" +
      "on duplicate key update click_num=click_num+?,click_dev_num=click_dev_num+?"
    val ps: PreparedStatement = conn.prepareStatement(sql2Mysql)
    //insert
    ps.setString(1, activeDate)
    ps.setInt(2, gameId)
    ps.setString(3, group_id)
    ps.setString(4, pkgCode)
    ps.setString(5, head_people)
    ps.setString(6, medium_account)
    ps.setInt(7, medium)
    ps.setString(8, idea_id)
    ps.setString(9, first_level)
    ps.setString(10, second_level)
    ps.setInt(11, clicks) //点击数
    ps.setInt(12, clickDevs) //点击设备数

    //update
    ps.setInt(13, clicks)
    ps.setInt(14, clickDevs)
    ps.executeUpdate()
    ps.close()


  }


  //插入点击数据
  def insertMomoClick(pkgCode: String, imei: String, ts: String, os: String, url: String, gameId: String, advName: Int, conn: Connection) = {
    val instSql = "insert into bi_ad_momo_click(pkg_id,imei,ts,os,callback,game_id,adv_name) values(?,?,?,?,?,?,?)"
    val ps: PreparedStatement = conn.prepareStatement(instSql)
    ps.setString(1, pkgCode)
    ps.setString(2, imei)
    ps.setString(3, ts)
    ps.setString(4, os)
    ps.setString(5, url)
    ps.setString(6, gameId)
    ps.setInt(7, advName)
    ps.executeUpdate()
    ps.close()
  }

  //更新陌陌单击时间ts
  def updateMomoClickTs(pkgCode: String, imei: String, ts: String, url: String, conn: Connection) = {
    val instSql = "update bi_ad_momo_click set ts=?,callback=? where pkg_id=? and imei=?"
    val ps: PreparedStatement = conn.prepareStatement(instSql)
    ps.setString(1, ts)
    ps.setString(2, url)
    ps.setString(3, pkgCode)
    ps.setString(4, imei)
    ps.executeUpdate()
    ps.close()
  }
}
