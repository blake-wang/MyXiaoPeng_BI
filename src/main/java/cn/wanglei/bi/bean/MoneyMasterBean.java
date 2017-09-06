package cn.wanglei.bi.bean;

/**
 * Created by Mr_yang on 2017/8/31.
 * 钱大师 点击 和 激活 bean类
 */

public class MoneyMasterBean {

    /**
     * ip : 192.268.22.189
     * imei : 5A5ZDF1EEEF2478D94EE709B98489589
     * pkg_id : 41FF12
     * adv_name : bi_adv_money_click
     * app_id : 223121
     * game_id : 414
     * ts : 2016-08-22 13:53:15
     */
    private String ip;
    private String imei;
    private String pkg_id;
    private String adv_name;
    private String app_id;
    private String game_id;
    private String ts;

    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setImei(String imei) {
        this.imei = imei;
    }

    public void setPkg_id(String pkg_id) {
        this.pkg_id = pkg_id;
    }

    public void setAdv_name(String adv_name) {
        this.adv_name = adv_name;
    }

    public void setApp_id(String app_id) {
        this.app_id = app_id;
    }

    public void setGame_id(String game_id) {
        this.game_id = game_id;
    }

    public void setTs(String ts) {
        this.ts = ts;
    }

    public String getIp() {
        return ip;
    }

    public String getImei() {
        return imei;
    }

    public String getPkg_id() {
        return pkg_id;
    }

    public String getAdv_name() {
        return adv_name;
    }

    public String getApp_id() {
        return app_id;
    }

    public String getGame_id() {
        return game_id;
    }

    public String getTs() {
        return ts;
    }

    @Override
    public String toString() {
        return "MoneyMasterBean{" +
                "ip='" + ip + '\'' +
                ", imei='" + imei + '\'' +
                ", pkg_id='" + pkg_id + '\'' +
                ", adv_name='" + adv_name + '\'' +
                ", app_id='" + app_id + '\'' +
                ", game_id='" + game_id + '\'' +
                ", ts='" + ts + '\'' +
                '}';
    }
}
