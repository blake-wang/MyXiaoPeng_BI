package cn.xiaopeng.bi.utils

import cn.wanglei.bi.utils.InsertMissInfo2RedisUtil
import org.apache.spark.rdd.RDD
import redis.clients.jedis.{Jedis, JedisPool}

/**
  * Created by kequan on 2/20/17.
  */
object AccountUtil {

  def deallogs(rdd: RDD[(String, String, String, String, String, String)]) = {
    rdd.distinct().foreachPartition(iter => {
      //数据库链接
      val conn = JdbcUtil.getConn();
      val stmtBi = conn.createStatement();

      val connxiaopeng2 = JdbcUtil.getXiaopeng2Conn();
      val stmt = connxiaopeng2.createStatement();

      // redis连接
      val pool: JedisPool = JedisUtil.getJedisPool
      val jedis: Jedis = pool.getResource

      iter.foreach(t => {
        //注册日志  header 游戏帐号 游戏id 注册时间
        //登录日志   header  游戏帐号 游戏id 登录时间
        val header = t._1;
        val game_account = t._2;
        var game_id_log = t._3;
        var user_type = t._5;
        val uid_bind = t._6;
        //数据格式排除
        //redis 补充数据的三个地方
        // 1.判断 game_account是否存在,没有的话补充账户数据：mysql:bgameaccount
        if (!jedis.exists(game_account.trim().toLowerCase())) {
          InsertMissInfo2RedisUtil.checkAccount(game_account, stmt, jedis)
        }
        var uid = "";
        if (user_type.equals("") && (!uid_bind.equals(""))) {
          //2.1:先注册帐号，再绑定黑金卡:此时基本信息在做注册的时候已经保留,需要取出 game_id;保存绑定的uid;如果绑定的是手机号  也要补救
          game_id_log = jedis.hget(game_account, "game_id");

          if (uid_bind.length < 11) {
            InsertMissInfo2RedisUtil.updateAccountUID(game_account, uid_bind, jedis);
          } else {
            InsertMissInfo2RedisUtil.checkAccountOwner(game_account, uid_bind, stmt, jedis)
          }
        } else {
          //(Uid_bind.equals("") && (!user_type.equals("")))
          //2.2 注册用户 :黑金卡注册 或者 常规注册,
          //有的数据已经存入，但是own_id 字段没有存入，也需要更新 ;如果绑定的是手机号  也要补救
          uid = jedis.hget(game_account.trim.toLowerCase(), "owner_id");
          if (uid == null || uid.equals("") || uid.equals("0")) {
            InsertMissInfo2RedisUtil.checkAccountUID(game_account, stmt, jedis)
          } else if (uid.length >= 11) {
            InsertMissInfo2RedisUtil.checkAccountOwner(game_account, uid, stmt, jedis)
          }
        }
        uid = jedis.hget(game_account.trim.toLowerCase(), "owner_id")
        //3.当通信证信息不存在且通行证格式正确的时候 补充通行证名称 ：mysql:member
        if ((!jedis.exists(uid + "_member")) && (uid != null)&& (!uid.equals("")) && (!uid.equals("0")) && uid.length < 11) {
          InsertMissInfo2RedisUtil.checkMember(uid, stmt, jedis)
        }

        //根据补充后的数据,过滤和完善数据
        //1.账户是否绑定通行证id 没有绑定 过滤
        //2.完善通行证信息,账户信息,游戏信息
        val status = jedis.hget(uid + "_member", "status")
        if ((uid != null) && (!uid.equals("")) && (!uid.equals("0")) && (uid.length < 11) && (status != null && (status.equals("2") || status.equals("3")))) {
          //作用1
          //作用2 完善游戏信息
          var game_id = jedis.hget(game_id_log + "_bgame", "mainid");
          var game_name = jedis.hget(game_id_log + "_bgame", "main_name");
          if (game_id == null || game_id.equals("")) {
            game_id = game_id_log;
            game_name = jedis.hget(game_id_log + "_bgame", "name");
          }
          if (game_name == null) {
            game_name = "";
          }
          //作用2 完善通行证信息
          var user_account = jedis.hget(uid + "_member", "username");
          if (user_account == null) {
            //该数据一定存在  只是防止测试环境数据杂乱
            user_account = "";
          }
          //作用2 完善账户信息
          var platform = jedis.hget(game_account, "reg_os_type"); //'UNKNOW','ANDROID','IOS'
          if (platform == null || platform.equals("")) {
            platform = "UNKNOW";
          }
          var is_recharge = 0;
          if (jedis.exists(game_account + "_is_order_no")) {
            is_recharge = 1;
          }
          if (header.contains("bi_regi")) {
            val regi_time = t._4;
            if ((user_type != null) && (user_type.equals("8"))) {
              user_type = "1";
            } else {
              user_type = "2";
            }
            if (game_account.length <= 20 && StringUtils.isNumber(game_id) && StringUtils.isNumber(uid) && game_name.length <= 100 && user_account.length <= 20) {
              //插入注册数据 game_account,uid,game_id,game_name,user_type,regi_time
              val sqlregi = "INSERT INTO bi_centurioncard_accountinfo(game_account,uid,game_id,game_name,user_type,user_account,platform,regi_time,is_recharge) VALUES ('" + game_account + "','" + uid + "','" + game_id + "','" + game_name + "','" + user_type + "','" + user_account + "','" + platform + "','" + regi_time + "','" + is_recharge + "') ON DUPLICATE KEY update game_id=VALUES(game_id),game_name=VALUES(game_name),user_type=VALUES(user_type),user_account=VALUES(user_account),platform=VALUES(platform),regi_time=(case when VALUES(regi_time)>=regi_time  then VALUES(regi_time) else regi_time end),is_recharge=VALUES(is_recharge)"
              stmtBi.executeUpdate(sqlregi);
            }
          }
          if (header.contains("bi_login")) {
            val last_login_time = t._4;
            if (game_account.length <= 20 && StringUtils.isNumber(game_id) && StringUtils.isNumber(uid) && game_name.length <= 100 && user_account.length <= 20) {
              // 插入登录数据 game_account,uid,game_id,game_name,last_login_time
              val sqllogin = "INSERT INTO bi_centurioncard_accountinfo(game_account,uid,game_id,game_name,user_account,platform,last_login_time,is_recharge) VALUES ('" + game_account + "','" + uid + "','" + game_id + "','" + game_name + "','" + user_account + "','" + platform + "','" + last_login_time + "','" + is_recharge + "') ON DUPLICATE KEY update game_id=VALUES(game_id),game_name=VALUES(game_name),user_account=VALUES(user_account),platform=VALUES(platform),last_login_time=(case when VALUES(last_login_time)>=last_login_time  then VALUES(last_login_time) else last_login_time end),is_recharge=VALUES(is_recharge)"
              stmtBi.executeUpdate(sqllogin);
            }
          }
          if (header.contains("bi_order")) {
            val last_login_time = t._4;
            if (game_account.length <= 20 && StringUtils.isNumber(game_id) && StringUtils.isNumber(uid) && game_name.length <= 100 && user_account.length <= 20) {
              // 插入支付数据 game_account,uid,game_id,is_recharge
              is_recharge = 1;
              val sqlorder = "INSERT INTO bi_centurioncard_accountinfo(game_account,uid,game_id,game_name,user_account,platform,is_recharge) VALUES ('" + game_account + "','" + uid + "','" + game_id + "','" + game_name + "','" + user_account + "','" + platform + "','" + is_recharge + "') ON DUPLICATE KEY update game_id=VALUES(game_id),game_name=VALUES(game_name),user_account=VALUES(user_account),platform=VALUES(platform),is_recharge=VALUES(is_recharge)"
              stmtBi.executeUpdate(sqlorder);
            }
          }
        }
      })

      // 关闭数据库
      stmt.close();
      stmtBi.close();
      connxiaopeng2.close();
      conn.close();
      //关闭redis
      pool.returnResource(jedis)
      pool.destroy()
    })
  }


}
