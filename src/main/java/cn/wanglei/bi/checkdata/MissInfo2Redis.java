package cn.wanglei.bi.checkdata;

import cn.xiaopeng.bi.utils.Commons;
import cn.xiaopeng.bi.utils.JdbcUtil;
import cn.xiaopeng.bi.utils.JedisUtil;
import cn.xiaopeng.bi.utils.SubCenturionDao;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by bigdata on 7/19/17.
 * 目的：由于redis中可能出现账号或者通行证等信息的遗漏，避免出现问题，半个小时监测一次
 */
public class MissInfo2Redis {
    /**
     * 对丢失的下级账号数据进行补充
     *
     * @param account
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static void checkAccountExists(String account, String loginTime) throws SQLException {
        Connection conn = JdbcUtil.getXiaopeng2Conn();
        Connection connbi = JdbcUtil.getConn();
        JedisPool pool = JedisUtil.getJedisPool();
        Jedis jedis = pool.getResource();

        String gameAccount = "";
        String gamiId = "0";
        String regiTime = "";
        String regiDate = "";
        String imei = "";
        String ip = "23.4.12.4";
        String promoCode = "";
        String userCode = "";
        String memberId = "";
        String userAccount = "";
        String pkgCode = "";
        int groupId = 0;
        String bindMember = "";
        String memberRegiTime = "";
        String sql = "\n" +
                "select spa.account,subordinate_code,bgm.addtime,bgm.imie,bgm.gameid,bgm.uid,puer.`code`,puer.username,puer.group_id,puer.create_time,mm.username bindMember,\n" +
                "bgm.channel_owner channel_id,bgm.os as reg_os_type,bind_member_id\n" +
                " from specialsrv_account spa join bgameaccount bgm on bgm.account=spa.account\n" +
                "join promo_user puer on puer.member_id=bgm.uid\n" +
                "left join member mm on mm.id=bgm.bind_member_id\n" +
                "where spa.account='accountList'".replace("accountList", account);
        System.out.println(sql);
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()) {
            gameAccount = rs.getString("account");
            gamiId = rs.getString("gameid");
            regiTime = rs.getString("addtime");
            regiDate = rs.getString("addtime").substring(0, 10);
            userCode = rs.getString("subordinate_code");
            imei = Commons.getImei(rs.getString("imie"));
            promoCode = rs.getString("code");
            memberId = rs.getString("uid");
            groupId = rs.getInt("group_id");
            memberRegiTime = rs.getString("create_time");
            bindMember = rs.getString("bindMember");
            pkgCode = promoCode + "~" + userCode;
            userAccount = rs.getString("username");
            SubCenturionDao.regiInfoProcessAccountBs(gameAccount, gamiId, regiTime, regiDate, imei, ip, promoCode, userCode, pkgCode, memberId, userAccount, groupId, bindMember, memberRegiTime, loginTime, connbi);
            SubCenturionDao.regiInfoProcessPkgStat(regiDate, promoCode, userCode, pkgCode, memberId, userAccount, gamiId, groupId, memberRegiTime, 1, 1, connbi);


            Map<String, String> accountredis = new HashMap<String, String>();
            accountredis.put("userid", memberId);
            accountredis.put("game_account", gameAccount.trim().toLowerCase());
            accountredis.put("game_id", gamiId);
            accountredis.put("reg_time", regiTime);
            accountredis.put("reg_resource", "8");
            accountredis.put("channel_id", rs.getString("channel_id") == null ? "0" : rs.getString("channel_id"));
            accountredis.put("owner_id", memberId);
            accountredis.put("bind_member_id", rs.getString("bind_member_id") == null ? "0" : rs.getString("bind_member_id"));
            accountredis.put("status", "1");
            accountredis.put("reg_os_type", rs.getString("reg_os_type") == null ? "UNKNOW" : rs.getString("reg_os_type"));
            accountredis.put("expand_code", promoCode);
            accountredis.put("expand_code_child", userCode);
            accountredis.put("expand_channel", "no-acc");
            if (gameAccount != null) {
                jedis.hmset(gameAccount.trim().toLowerCase(), accountredis);
            }


        }
        stmt.close();
        conn.close();
        connbi.close();
        pool.returnResource(jedis);
        pool.close();


    }


    //帐号补充
    public static int checkAccount(String accountList) throws SQLException {
        Connection conn = JdbcUtil.getXiaopeng2Conn();
        int flag = 1;
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
            flag = 0;
        }
        String sql = "SELECT '00' requestid,'' as userid,a.account as game_account,a.gameid as  game_id,a.addtime as reg_time,'' reg_resource,a.account_channel as  channel_id,\n" +
                " a.uid as owner_id,a.bind_member_id as bind_member_id,a.state status,if(a.os='','UNKNOW',os) as reg_os_type,if(b.code is null,0,b.code) as expand_code,'no-acc' as expand_channel,subordinate_code FROM bgameaccount a \n" +
                " left join promo_user b on a.uid=b.member_id \n" +
                " left join specialsrv_account sa on sa.account=a.account\n" +
                " where a.account in('accountList')".replace("accountList", accountList);
        ResultSet rs = stmt.executeQuery(sql);
        try {
            JedisPool pool = JedisUtil.getJedisPool();
            Jedis jedis = pool.getResource();
            while (rs.next()) {
                Map<String, String> account = new HashMap<String, String>();
                account.put("requestid", rs.getString("requestid") == null ? "" : rs.getString("requestid"));
                account.put("userid", rs.getString("userid") == null ? "" : rs.getString("userid"));
                account.put("game_account", rs.getString("game_account") == null ? "" : rs.getString("game_account").trim().toLowerCase());
                account.put("game_id", rs.getString("game_id") == null ? "0" : rs.getString("game_id"));
                account.put("reg_time", rs.getString("reg_time") == null ? "0000-00-00" : rs.getString("reg_time"));
                account.put("reg_resource", rs.getString("reg_resource") == null ? "2" : rs.getString("reg_resource"));
                account.put("channel_id", rs.getString("channel_id") == null ? "0" : rs.getString("channel_id"));
                account.put("owner_id", rs.getString("owner_id") == null ? "0" : rs.getString("owner_id"));
                account.put("bind_member_id", rs.getString("bind_member_id") == null ? "0" : rs.getString("bind_member_id"));
                account.put("status", rs.getString("status") == null ? "1" : rs.getString("status"));
                account.put("reg_os_type", rs.getString("reg_os_type") == null ? "UNKNOW" : rs.getString("reg_os_type"));
                account.put("expand_code", rs.getString("expand_code") == null ? "" : rs.getString("expand_code"));
                account.put("expand_code_child", rs.getString("subordinate_code") == null ? "" : rs.getString("subordinate_code"));
                account.put("expand_channel", rs.getString("expand_channel"));
                if (rs.getString("game_account") != null) {
                    jedis.del(rs.getString("game_account").toLowerCase().trim());
                    jedis.hmset(rs.getString("game_account").trim().toLowerCase(), account);
                }
            }
            stmt.close();
            conn.close();
            pool.returnResource(jedis);
            pool.destroy();
        } catch (SQLException e) {
            System.out.println(e);
            flag = 0;
        }
        return flag;
    }
}
