package cn.wanglei.bi.utils;

import cn.xiaopeng.bi.utils.JdbcUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import testdemo.redis.JedisUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by bigdata on 17-8-18.
 * 由于redis中可能出现帐号或者通行证等信息的遗漏，避免出现问题，半个小时监测一次
 */
public class MissInfo2Redis {
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
        String sql = "select '00' requestid,'' as userid,a.account as game_account,a.gameid as game_id,a.addtime as reg_time,'' reg_resource,a.account_channel as channel_id,a.uid as owner_id,a.bind_member as bind_member_id,a.state status,if(a.os='','UNKNOW',os) from bgameaccount a left join promo_user b on a.uid=b.member_id where a.account in ('accountList')".replace("accountList", accountList);
        ResultSet rs = stmt.executeQuery(sql);
        try {
            JedisPool pool = JedisUtil.getJedisPool();
            Jedis jedis = pool.getResource();
            while(rs.next()){
                Map<String,String> account = new HashMap<String,String>();
                account.put("requestid",rs.getString("requestid")==null? "":rs.getString("requestid"));
                account.put("userid",rs.getString("userid")==null?"":rs.getString("userid"));
                account.put("game_account",rs.getString("game_account")==null?"":rs.getString("game_account").trim().toLowerCase());
                account.put("game_id",rs.getString("game_id")==null?"0":rs.getString("game_id"));
                account.put("reg_time",rs.getString("reg_time")==null?"0000-00-00":rs.getString("reg_time"));
                account.put("reg_resource",rs.getString("reg_resource")==null?"2":rs.getString("reg_resource"));
                account.put("channel_id",rs.getString("channel_id")==null?"0":rs.getString("channel_id"));
                account.put("owner_id",rs.getString("owner_id")==null?"0":rs.getString("owner_id"));
                account.put("bind_member_id",rs.getString("bind_member_id")==null?"0":rs.getString("bind_member_id"));
                account.put("status",rs.getString("status")==null?"1":rs.getString("status"));
                account.put("reg_os_type",rs.getString("reg_os_type")==null?"UNKNOW":rs.getString("reg_os_type"));
                account.put("expand_code",rs.getString("expand_code")==null?"":rs.getString("expand_code"));
                account.put("expand_channel",rs.getString("expand_channel"));
                if(rs.getString("game_account")!=null){
                    jedis.hmset(rs.getString("game_account").trim().toLowerCase(),account);
                }
            }
            stmt.close();
            conn.close();
            pool.returnResource(jedis);
            pool.destroy();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(e);
            flag=0;
        }
        return flag;
    }
}
