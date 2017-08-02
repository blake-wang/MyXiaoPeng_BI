package cn.wanglei.bi.utils;

import cn.xiaopeng.bi.utils.DateUtils;
import cn.xiaopeng.bi.utils.JdbcUtil;
import cn.xiaopeng.bi.utils.JedisUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by bigdata on 17-8-1.
 * <p>
 * 目的：从mysql中定时查询数据，然后推送到redis中，发行相关维度数据到redis
 */

public class PublicFxGgameTbPush2Redis {
    public static void publicGgameTbPush2Redis() throws SQLException {
        Connection conn = JdbcUtil.getXiaopeng2FXConn();
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        JedisPool pool = JedisUtil.getJedisPool();
        Jedis jedis = pool.getResource();
        fx2Dim(conn, stmt, jedis);
        fx2DimGame(conn, stmt, jedis);
        conn.close();
        pool.returnResource(jedis);
        pool.destroy();

    }

    /**
     * 推送主游戏和组
     *
     * @param conn
     * @param stmt
     * @param jedis
     */
    private static void fx2DimGame(Connection conn, Statement stmt, Jedis jedis) throws SQLException {
        //发行主游戏数据加载到后面
        String sqlM = "select distinct old_game_id as id,game_id as mainid,sdk.system_type,base.game_name main_name,sdk.group_id publish_group_id from game_sdk sdk join game_base base on base.id=sdk.game_id";
        stmt = conn.createStatement();
        ResultSet rsM = stmt.executeQuery(sqlM);
        while (rsM.next()) {
            Map<String, String> game_main = new HashMap<String, String>();
            game_main.put("mainid", rsM.getString("mainid") == null ? "" : rsM.getString("mainid"));
            game_main.put("system_type", rsM.getString("system_type") == null ? "" : rsM.getString("system_type"));
            game_main.put("main_name", rsM.getString("main_name") == null ? "" : rsM.getString("main_name"));
            game_main.put("publish_group_id", rsM.getString("publish_group_id") == null ? "" : rsM.getString("publish_group_id"));
            jedis.hmset(rsM.getString("id") + "_publish_game", game_main);
        }


    }

    /**
     * 发行维度数据，推广帐号，负责人，推广渠道，等等
     *
     * @param conn
     * @param stmt
     * @param jedis
     */
    private static void fx2Dim(Connection conn, Statement stmt, Jedis jedis) throws SQLException {
        //媒介帐号
        String sqla = "select subpackage_id as pkg_code,a.merchant_id,merchant from medium_package a join merchant b on a.merchant_id=b.merchant_id ";
        ResultSet rs = stmt.executeQuery(sqla);
        while (rs.next()) {
            Map<String, String> merchant = new HashMap<String, String>();
            merchant.put("pkg_code", rs.getString("pkg_code") == null ? "0" : rs.getString("pkg_code"));
            merchant.put("medium_account", rs.getString("merchant") == null ? "0" : rs.getString("merchant"));
            jedis.hmset(rs.getString("pkg_code") + "_pkgcode", merchant);//加载到redis

        }
        //推广渠道1
        String sqlc1 = "select distinct pkg_code,main_name promotion_channel from channel_pkg a join channel_main b on b.id=a.main_id ";
        rs = stmt.executeQuery(sqlc1);
        while (rs.next()) {
            Map<String, String> tgqd1 = new HashMap<String, String>();
            tgqd1.put("pkg_code", rs.getString("pkg_code") == null ? "" : rs.getString("pkg_code"));
            tgqd1.put("promotion_channel", rs.getString("promotion_channel") == null ? "" : rs.getString("promotion_channel"));
            jedis.hmset(rs.getString("pkg_code") + "_pkgcode", tgqd1);
        }

        //推广渠道2
        String sqlc2 = "select subpackage_id pkg_code,agent_name promotion_channel from medium_package a \n" +
                "join merchant b on a.merchant_id=b.merchant_id join agent c on c.id=b.agent_id  ";
        rs = stmt.executeQuery(sqlc2);
        while (rs.next()) {
            Map<String, String> tgqd1 = new HashMap<String, String>();
            tgqd1.put("pkg_code", rs.getString("pkg_code") == null ? "" : rs.getString("pkg_code"));
            tgqd1.put("promotion_channel", rs.getString("promotion_channel") == null ? "" : rs.getString("promotion_channel"));
            jedis.hmset(rs.getString("pkg_code") + "_pkgcode", tgqd1); //加载到redis
        }

        //推广负责人1
        for (int ii = -1; ii <= 1; ii++) {
            String dt = DateUtils.getDay(ii);
            String sqlms = "select pkg_code,? as dt,c.name head_people  from channel_pkg a join channel_manager b on a.channel_id=b.channel_id join user c on c.id=b.manager\n" +
                    "where left(b.start_time,10)<=? and left(b.end_time,10)>=?";

            PreparedStatement ps = conn.prepareStatement(sqlms);
            ps.setString(1, dt);
            ps.setString(2, dt);
            ps.setString(3, dt);
            rs = ps.executeQuery();
            while (rs.next()) {
                Map<String, String> tgqd1 = new HashMap<String, String>();
                tgqd1.put("pkg_code", rs.getString("pkg_code") == null ? "" : rs.getString("pkg_code"));
                tgqd1.put("head_people", rs.getString("head_people") == null ? "" : rs.getString("head_people"));
                jedis.hmset(rs.getString("pkg_code") + "_" + dt + "_pkgcode", tgqd1);
                jedis.expire(rs.getString("pkg_code") + "_" + dt + "_pkgcode", 3600 * 24 * 3);
            }
        }
        //推广负责人2
        for (int ii = -1; ii <= 1; ii++) {
            String dt = DateUtils.getDay(ii);
            String sqlms = "select subpackage_id as pkg_code,? as dt,us.name head_people  from medium_package a join merchant b on a.merchant_id=b.merchant_id \n" +
                    "join charge_log cl on cl.source_id=b.id and family=3 join user us on us.id=cl.user_id and us.state=1  where left(cl.start_time,10)<=? and left(cl.end_time,10)>=? ";
            System.out.println(sqlms);
            PreparedStatement ps = conn.prepareStatement(sqlms);
            ps.setString(1, dt);
            ps.setString(2, dt);
            ps.setString(3, dt);
            rs = ps.executeQuery();
            while (rs.next()) {
                Map<String, String> tgqd1 = new HashMap<String, String>();
                tgqd1.put("pkg_code", rs.getString("pkg_code") == null ? "" : rs.getString("pkg_code"));
                tgqd1.put("head_people", rs.getString("head_people") == null ? "" : rs.getString("head_people"));
                jedis.hmset(rs.getString("pkg_code") + "_" + dt + "_pkgcode", tgqd1); //加载到redis
                jedis.expire(rs.getString("pkg_code") + "_" + dt + "_pkgcode", 3600 * 24 * 3);
            }
        }

        //推广模式
        for (int ii = -1; ii <= 1; ii++) {
            String dt = DateUtils.getDay(ii);
            String sqlms = "select pkg_code,? as dt,b.promotion promotion_mode from channel_pkg a join channel_pkg_conf b on a.id=b.pkg_id\n" +
                    "where left(b.start_time,10)<=? and left(b.end_time,10)>=?";
            PreparedStatement ps = conn.prepareStatement(sqlms);
            ps.setString(1, dt);
            ps.setString(2, dt);
            ps.setString(3, dt);
            rs = ps.executeQuery();
            while (rs.next()) {
                Map<String, String> tgqd1 = new HashMap<String, String>();
                tgqd1.put("pkg_code", rs.getString("pkg_code") == null ? "" : rs.getString("pkg_code"));
                tgqd1.put("promotion_mode", rs.getString("promotion_mode") == null ? "" : rs.getString("promotion_mode"));
                jedis.hmset(rs.getString("pkg_code") + "_" + dt + "_pkgcode", tgqd1); //加载到redis
                jedis.expire(rs.getString("pkg_code") + "_" + dt + "_pkgcode", 3600 * 24 * 3);
            }
        }


    }


}
