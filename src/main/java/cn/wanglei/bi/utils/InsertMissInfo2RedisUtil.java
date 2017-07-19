package cn.wanglei.bi.utils;

import redis.clients.jedis.Jedis;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by bigdata on 7/18/17.
 */
public class InsertMissInfo2RedisUtil {

    /**
     * @param accounts
     * @throws SQLException
     * @throws ClassNotFoundException
     * @function: 监测redis中是否存在程序找不到的会员，若有则取出从库中补全
     */
    public static int checkAccount(String accounts, Statement stmt, Jedis jedis) throws SQLException {
        int flag = 1;
        //子游戏表数据
        String sql = "SELECT '00' requestid,'' as userid,a.account as game_account,a.gameid as  game_id,a.addtime as reg_time,'' reg_resource,a.account_channel as  channel_id,a.uid as owner_id,a.bind_member_id as bind_member_id,a.state status,if(a.os='','UNKNOW',os) as reg_os_type,'0' as expand_code,'0' as expand_channel FROM bgameaccount a left join promo_user b on a.uid=b.member_id where a.account in('accounts')".replace("accounts", accounts);
        ResultSet rs = stmt.executeQuery(sql);
        try {
            while(rs.next()){
                Map<String,String> account = new HashMap();
                account.put("requestid",rs.getString("requesteid")==null? "" : rs.getString("requestid"));
                account.put("userid", rs.getString("userid") == null ? "" : rs.getString("userid"));
                account.put("game_account", rs.getString("game_account") == null ? "" : rs.getString("game_account").trim().toLowerCase());
                account.put("game_id", rs.getString("game_id") == null ? "0" : rs.getString("game_id"));
                account.put("reg_time", rs.getString("reg_time") == null ? "0000-00-00" : rs.getString("reg_time"));
                account.put("reg_resource", rs.getString("reg_resource") == null ? "" : rs.getString("reg_resource"));
                account.put("channel_id", rs.getString("channel_id") == null ? "0" : rs.getString("channel_id"));
                account.put("owner_id", rs.getString("owner_id") == null ? "0" : rs.getString("owner_id"));
                account.put("bind_member_id", rs.getString("bind_member_id") == null ? "0" : rs.getString("bind_member_id"));
                account.put("status", rs.getString("status") == null ? "1" : rs.getString("status"));
                account.put("reg_os_type", rs.getString("reg_os_type") == null ? "UNKNOW" : rs.getString("reg_os_type"));
                account.put("expand_code", rs.getString("expand_code") == null ? "0" : rs.getString("expand_code"));
                account.put("expand_channel", rs.getString("expand_channel") == null ? "" : rs.getString("expand_channel"));
                if(rs.getString("game_account")!=null){
                    jedis.del(rs.getString("game_account").toLowerCase().trim());
                    jedis.hmset(rs.getString("game_account").trim().toLowerCase(),account);
                }

            }
        } catch (Exception e){
            System.out.println(e);
            flag = 0;
        }

        return flag;

    }
    /**
     * @param accounts
     * @throws SQLException
     * @throws ClassNotFoundException
     * @function: 监测redis中是否存在程序找不到的会员，若有则取出从库中补全
     */


    public static int updateAccountUID(String accounts,String owner_id,Jedis jedis){
        int flag = 1;
        //子游戏表数据
        try {
            jedis.hset(accounts.trim().toLowerCase(),"owner_id",owner_id.trim());
        } catch (Exception e) {
            System.out.println(e);
            flag = 0;

        }

        return flag;
    }

    /**
     * @param accounts,memberid
     * @throws SQLException
     * @throws ClassNotFoundException
     * @function: 监测redis中是否存在程序找不到的会员，若有则取出从库中补全
     */

    public static int checkAccountOwner(String accounts,String ownerid,Statement stmt,Jedis jedis) throws SQLException {
        int flag = 1;
        String sql = "SELECT id as owner_id FROM member a where a.username in('ownerid')".replace("ownerid", ownerid);
        ResultSet rs = stmt.executeQuery(sql);
        try {
            while(rs.next()){
                Map<String,String> account = new HashMap();
                account.put("game_account",accounts.trim().toLowerCase());
                account.put("owner_id",rs.getString("owner_id")==null? "0":rs.getString("owner_id"));
                if(accounts != null){
                    jedis.hmset(accounts.trim().toLowerCase(),account);
                }
            }
        } catch (SQLException e) {
            System.out.println(e);
            flag = 0;
        }
        return flag;
    }


    /**
     * @param accounts
     * @throws SQLException
     * @throws ClassNotFoundException
     * @function: 监测redis中是否存在程序找不到的会员，若有则取出从库中补全
     */
    public static int checkAccountUID(String accounts,Statement stmt,Jedis jedis) throws SQLException {
        int flag = 1;
        //子游戏表数据
        String sql =  "SELECT a.account as game_account, a.uid as owner_id FROM bgameaccount a left join promo_user b on a.uid=b.member_id where a.account in('accounts')".replace("accounts", accounts.trim().toLowerCase());
        ResultSet rs = stmt.executeQuery(sql);
        try {
            while(rs.next()){
                if(rs.getString("game_account")!=null){
                    jedis.hset(rs.getString("game_account").trim().toLowerCase(),"owner_id",rs.getString("owner_id").trim());
                }
            }
        } catch (SQLException e) {
            System.out.println(e);
            flag = 0;
        }
        return flag;
    }

    /**
     * @param memberid
     * @throws SQLException
     * @throws ClassNotFoundException
     * @function: 监测redis中是否存在程序找不到的会员，若有则取出从库中补全
     */
    public static int checkMember(String memberid,Statement stmt,Jedis jedis) throws SQLException {
        int flag = 1;
        //子游戏表数据
        String sql = "select mm.id,mm.username,mm.tel,mm.qq,sex,`desc` as descr,email,regtype,pu.member_grade grade,vip_note,pu.`code` as promo_code,pu.member_id promo_member_id,pu.`code` invite_code,regtime,'1' as status \n" +
                "from member mm left join promo_user pu  on pu.member_id=mm.id where mm.id in(memberid) ".replace("memberid", memberid);
        ResultSet rs =stmt.executeQuery(sql);
        try {
            while(rs.next()){
                Map<String,String> member = new HashMap<String, String>();
                member.put("username", rs.getString("username") == null ? "" : rs.getString("username"));
                member.put("tel", rs.getString("tel") == null ? "" : rs.getString("tel"));
                member.put("qq", rs.getString("qq") == null ? "" : rs.getString("qq"));
                member.put("sex", rs.getString("sex") == null ? "0" : rs.getString("sex"));
                member.put("descr", rs.getString("descr") == null ? "" : rs.getString("descr"));
                member.put("email", rs.getString("email") == null ? "" : rs.getString("email"));
                member.put("regtype", rs.getString("regtype") == null ? "0" : rs.getString("regtype"));  //注册类型
                member.put("grade", rs.getString("grade") == null ? "" : rs.getString("grade"));   //通行证等级
                member.put("vip_note", rs.getString("vip_note") == null ? "" : rs.getString("vip_note"));
                member.put("promo_code", rs.getString("promo_code") == null ? "0" : rs.getString("promo_code"));  //黑金卡推广码
                member.put("promo_member_id", rs.getString("promo_member_id") == null ? "0" : rs.getString("promo_member_id"));
                member.put("invite_code", rs.getString("invite_code") == null ? "" : rs.getString("invite_code"));  //邀请码
                member.put("regtime", rs.getString("regtime") == null ? "" : rs.getString("regtime")); //注册时间
                member.put("status", rs.getString("status") == null ? "1" : rs.getString("status"));
                jedis.hmset(rs.getString("id").trim() + "_member", member); //加载到redis
                if(rs.getString("username")!=null){
                    jedis.set(rs.getString("username"),rs.getString("id").trim());
                }
                if(rs.getString("promo_code")!=null){
                    jedis.set(rs.getString("promo_code"),rs.getString("id").trim());
                }
            }
        } catch (SQLException e) {
            System.out.println(e);
            flag =0;
        }
        return flag;

    }

}
