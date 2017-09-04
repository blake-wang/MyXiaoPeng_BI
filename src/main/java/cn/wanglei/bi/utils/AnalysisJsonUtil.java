package cn.wanglei.bi.utils;

import cn.wanglei.bi.bean.AndroidErrorBean;
import cn.wanglei.bi.bean.GameInsideRoleData;
import cn.wanglei.bi.bean.MoneyMasterBean;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import java.io.IOException;
import java.io.Serializable;
import java.util.zip.DataFormatException;

/**
 * Created by bigdata on 17-9-4.
 */
public class AnalysisJsonUtil implements Serializable {
    public static Gson gson = new Gson();


    public static MoneyMasterBean AnalysisMoneyMasterData(String data) {
        try {
            return gson.fromJson(data, MoneyMasterBean.class);
        } catch (JsonSyntaxException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static GameInsideRoleData AnalysisGameInsideData(String data) {
        try {
            return gson.fromJson(data, GameInsideRoleData.class);
        } catch (JsonSyntaxException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static AndroidErrorBean DecodingAndAnalysisAndroidError(String error) {
        try {
            return gson.fromJson(DecodindlogUtil.serviceDecode(error), AndroidErrorBean.class);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
