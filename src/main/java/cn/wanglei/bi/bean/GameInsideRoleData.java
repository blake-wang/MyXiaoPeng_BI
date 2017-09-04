package cn.wanglei.bi.bean;

/**
 * Created by kequan on 3/27/17.
 */
public class GameInsideRoleData {

    /**
     * title : {"gameid":"","ver":"5.1","os":"5.1","lid":"","roleid":"8521215132458","ip":"182.41.193.235","usertype":"","servareaname":"朋友玩#416服 千里奔袭","userid":"pywan_f3730d45e474cfca2b76c53b1f161d2e","gamelevel":"","tid":"","logclass":"BI_ROLE","network":"wifi","servarea":"ly1984","userlevel":"4","imei":"44:04:44:c9:73:66","rolelevel":"4","model":"OPPO A59m","exectime":"2017-05-24 18:20:02"}
     * content : {"roleid":"8521215132458","rolename":"嵇稀","rolelevel":null,"operatime":"2017-05-24 18:20:02","roletype":"神剑","rolechgype":"1","status":"1"}
     */
    private TitleEntity title;
    private ContentEntity content;

    public void setTitle(TitleEntity title) {
        this.title = title;
    }

    public void setContent(ContentEntity content) {
        this.content = content;
    }

    public TitleEntity getTitle() {
        return title;
    }

    public ContentEntity getContent() {
        return content;
    }

    public class TitleEntity {
        /**
         * gameid :
         * ver : 5.1
         * os : 5.1
         * lid :
         * roleid : 8521215132458
         * ip : 182.41.193.235
         * usertype :
         * servareaname : 朋友玩#416服 千里奔袭
         * userid : pywan_f3730d45e474cfca2b76c53b1f161d2e
         * gamelevel :
         * tid :
         * logclass : BI_ROLE
         * network : wifi
         * servarea : ly1984
         * userlevel : 4
         * imei : 44:04:44:c9:73:66
         * rolelevel : 4
         * model : OPPO A59m
         * exectime : 2017-05-24 18:20:02
         */
        private String gameid;
        private String ver;
        private String os;
        private String lid;
        private String roleid;
        private String ip;
        private String usertype;
        private String servareaname;
        private String userid;
        private String gamelevel;
        private String tid;
        private String logclass;
        private String network;
        private String servarea;
        private String userlevel;
        private String imei;
        private String rolelevel;
        private String model;
        private String exectime;

        public void setGameid(String gameid) {
            this.gameid = gameid;
        }

        public void setVer(String ver) {
            this.ver = ver;
        }

        public void setOs(String os) {
            this.os = os;
        }

        public void setLid(String lid) {
            this.lid = lid;
        }

        public void setRoleid(String roleid) {
            this.roleid = roleid;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public void setUsertype(String usertype) {
            this.usertype = usertype;
        }

        public void setServareaname(String servareaname) {
            this.servareaname = servareaname;
        }

        public void setUserid(String userid) {
            this.userid = userid;
        }

        public void setGamelevel(String gamelevel) {
            this.gamelevel = gamelevel;
        }

        public void setTid(String tid) {
            this.tid = tid;
        }

        public void setLogclass(String logclass) {
            this.logclass = logclass;
        }

        public void setNetwork(String network) {
            this.network = network;
        }

        public void setServarea(String servarea) {
            this.servarea = servarea;
        }

        public void setUserlevel(String userlevel) {
            this.userlevel = userlevel;
        }

        public void setImei(String imei) {
            this.imei = imei;
        }

        public void setRolelevel(String rolelevel) {
            this.rolelevel = rolelevel;
        }

        public void setModel(String model) {
            this.model = model;
        }

        public void setExectime(String exectime) {
            this.exectime = exectime;
        }

        public String getGameid() {
            return gameid;
        }

        public String getVer() {
            return ver;
        }

        public String getOs() {
            return os;
        }

        public String getLid() {
            return lid;
        }

        public String getRoleid() {
            return roleid;
        }

        public String getIp() {
            return ip;
        }

        public String getUsertype() {
            return usertype;
        }

        public String getServareaname() {
            return servareaname;
        }

        public String getUserid() {
            return userid;
        }

        public String getGamelevel() {
            return gamelevel;
        }

        public String getTid() {
            return tid;
        }

        public String getLogclass() {
            return logclass;
        }

        public String getNetwork() {
            return network;
        }

        public String getServarea() {
            return servarea;
        }

        public String getUserlevel() {
            return userlevel;
        }

        public String getImei() {
            return imei;
        }

        public String getRolelevel() {
            return rolelevel;
        }

        public String getModel() {
            return model;
        }

        public String getExectime() {
            return exectime;
        }
    }

    public class ContentEntity {
        /**
         * roleid : 8521215132458
         * rolename : 嵇稀
         * rolelevel : null
         * operatime : 2017-05-24 18:20:02
         * roletype : 神剑
         * rolechgype : 1
         * status : 1
         */
        private String roleid;
        private String rolename;
        private String rolelevel;
        private String operatime;
        private String roletype;
        private String rolechgype;
        private String status;

        public void setRoleid(String roleid) {
            this.roleid = roleid;
        }

        public void setRolename(String rolename) {
            this.rolename = rolename;
        }

        public void setRolelevel(String rolelevel) {
            this.rolelevel = rolelevel;
        }

        public void setOperatime(String operatime) {
            this.operatime = operatime;
        }

        public void setRoletype(String roletype) {
            this.roletype = roletype;
        }

        public void setRolechgype(String rolechgype) {
            this.rolechgype = rolechgype;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public String getRoleid() {
            return roleid;
        }

        public String getRolename() {
            return rolename;
        }

        public String getRolelevel() {
            return rolelevel;
        }

        public String getOperatime() {
            return operatime;
        }

        public String getRoletype() {
            return roletype;
        }

        public String getRolechgype() {
            return rolechgype;
        }

        public String getStatus() {
            return status;
        }
    }
}
