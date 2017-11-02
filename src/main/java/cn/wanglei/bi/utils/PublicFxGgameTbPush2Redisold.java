package cn.wanglei.bi.utils;

import cn.xiaopeng.bi.utils.DateUtils;
import cn.xiaopeng.bi.utils.JdbcUtil;
import cn.xiaopeng.bi.utils.JedisUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by bigdata on 17-8-1.
 * <p>
 * 目的：从mysql中定时查询数据，然后推送到redis中，发行相关维度数据到redis
 */

public class PublicFxGgameTbPush2Redisold {


    //测试redis存入取出数据
    public static void main(String[] args) throws SQLException {

        JedisPool pool = JedisUtil.getJedisPool();
        Jedis jedis = pool.getResource();
        jedis.select(10);

        publicGameTbPush2Redis();

        printMainId(jedis);


    }

    //把全量+实时日志，深度联运转化-临时表
    public static void publicGameTbPush2Redis() throws SQLException {
        Connection conn = JdbcUtil.getXiaopeng2FXConn();
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        JedisPool pool = JedisUtil.getJedisPool();
        Jedis jedis = pool.getResource();
        jedis.select(10);

        fx2DimGame(conn, stmt, jedis);

        conn.close();
        pool.returnResource(jedis);
        pool.destroy();
    }


    //推送主游戏和组
    private static void fx2DimGame(Connection conn, Statement stmt, Jedis jedis) throws SQLException {
        //发行主游戏数据加载到后面  old_game_id 是子游戏id，game_id是主游戏 id
        String sqlM = "select distinct old_game_id as id,game_id as mainid,sdk.system_type,base.game_name main_name,sdk.group_id publish_group_id from game_sdk sdk join game_base base on base.id=sdk.game_id";
        System.out.println(sqlM);
        stmt = conn.createStatement();
        ResultSet resultSet = stmt.executeQuery(sqlM);
        while (resultSet.next()) {
            System.out.println(resultSet.getString("id"));
            Map<String, String> game_main = new HashMap<String, String>();
            game_main.put("mainid", resultSet.getString("mainid") == null ? "" : resultSet.getString("mainid"));
            game_main.put("system_type", resultSet.getString("system_type") == null ? "" : resultSet.getString("system_type"));
            game_main.put("main_name", resultSet.getString("main_name") == null ? "" : resultSet.getString("main_name"));
            game_main.put("publish_group_id", resultSet.getString("publish_group_id") == null ? "" : resultSet.getString("publish_group_id"));
            jedis.hmset(resultSet.getString("id") + "_publish_game", game_main);
        }
    }

    //媒介帐号
    private static void medium_package(Connection conn, Statement stmt, Jedis jedis) throws SQLException {
        //媒介帐号
        String sqla = "select subpackage_id as pkg_code,a.merchant_id,merchant from medium_package a join merchant b on a.merchant_id=b.merchant_id ";
        System.out.println(sqla);
        ResultSet resultSet = stmt.executeQuery(sqla);
        while (resultSet.next()) {
            Map<String, String> merchant = new HashMap<String, String>();
            merchant.put("pkg_code", resultSet.getString("pkg_code") == null ? "0" : resultSet.getString("pkg_code"));
            merchant.put("medium_account", resultSet.getString("medium_account") == null ? "0" : resultSet.getString("medium_account"));
            jedis.hmset(resultSet.getString("pkg_code") + "_pkgcode", merchant);
        }
    }

    //推广渠道1
    private static void TGQD1(Connection conn, Statement stmt, Jedis jedis) throws SQLException {
        String sqlc1 = "select distinct pkg_code,main_name promotion_channel from channel_pkg a join channel_main b on b.id=a.main_id ";
        System.out.println("推广渠道1：" + sqlc1);
        ResultSet resultSet = stmt.executeQuery(sqlc1);
        while (resultSet.next()) {
            Map<String, String> tgqd1 = new HashMap<String, String>();
            tgqd1.put("pkg_code", resultSet.getString("pkg_code") == null ? "" : resultSet.getString("pkg_code"));
            tgqd1.put("promotion_channel", resultSet.getString("promotion_channel") == null ? "" : resultSet.getString("promotion_channel"));
            jedis.hmset(resultSet.getString("pkg_code") + "_pkgcode", tgqd1);
        }
    }

    //推广渠道2
    private static void TGQD2(Connection conn, Statement stmt, Jedis jedis) throws SQLException {
        String sqlc2 = "select subpackage_id pkg_code,agent_name promotion_channel from medium_package a \n" +
                "join merchant b on a.merchant_id=b.merchant_id join agent c on c.id=b.agent_id  ";
        System.out.println("推广渠道2：" + sqlc2);
        ResultSet resultSet = stmt.executeQuery(sqlc2);
        while (resultSet.next()) {
            Map<String, String> tgqd2 = new HashMap<String, String>();
            tgqd2.put("pkg_code", resultSet.getString("pkg_code") == null ? "" : resultSet.getString("pkg_code"));
            tgqd2.put("promotion_channel", resultSet.getString("promotion_channel") == null ? "" : resultSet.getString("promotion_channel"));
            jedis.hmset(resultSet.getString("pkg_code") + "_pkgcode", tgqd2);
        }
    }




    //打印主游戏和组
    private static void printMainId(Jedis jedis) {
        String mainid = jedis.hget("3865_publish_game", "mainid");
        String system_type = jedis.hget("3865_publish_game", "system_type");
        String main_name = jedis.hget("3865_publish_game", "main_name");
        String publish_group_id = jedis.hget("3865_publish_game", "publish_group_id");

        System.out.println("mainid  :  " + mainid);
        System.out.println("system_type  :  " + system_type);
        System.out.println("main_name  :  " + main_name);
        System.out.println("publish_group_id  :  " + publish_group_id);

//        List<String> resulet = jedis.hmget("3865_publish_game", "mainid", "system_type", "main_name", "publish_group_id");
//        int i = 0;
//        for (String s : resulet) {
//            i++;
//            System.out.println(s + "   --     " + i);
//        }
    }


}
