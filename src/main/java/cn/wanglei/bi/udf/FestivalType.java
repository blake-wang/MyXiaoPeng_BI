package cn.wanglei.bi.udf;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cn.wanglei.bi.ConfigurationUtil;
import org.apache.hadoop.hive.ql.exec.UDF;

import static cn.wanglei.bi.ConfigurationUtil.getEnvProperty;

public class FestivalType extends UDF{

    private HashMap<String,String> festivalMap = new HashMap<String, String>();

    public FestivalType() throws Exception {
//        Properties prop = new Properties();
//        InputStreamReader propFile = new InputStreamReader(getClass().getClassLoader().getResourceAsStream("festival_date.properties"), "UTF-8");
//
//        prop.load(propFile);


        Properties prop = new Properties();
        InputStream in = ConfigurationUtil.class.getClassLoader().getResourceAsStream("festival_date.properties");
        try {
            prop.load(in);
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        for(Object key:prop.keySet()) {
            festivalMap.put(key.toString(), prop.getProperty(key.toString()));
        }
    }

    //解决double转string的科学计数法的问题
    public String double_to_string(double dou) {
        Double dou_obj = new Double(dou);
        NumberFormat nf = NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        String dou_str = nf.format(dou_obj);
        return dou_str;
    }

    public String evaluate(double date_dou) {
        String date_str = this.double_to_string(date_dou);
        return evaluate(date_str);
    }

    public int evaluate(double date_dou,String flag) {
        String date_str = this.double_to_string(date_dou);
        return evaluate(date_str,flag);
    }

    public String evaluate(String date_str) {
        if (! this.match_date(date_str).equals("null")) {
            date_str = this.match_date(date_str);
            return festivalMap.get(date_str) == null ? "null" : festivalMap.get(date_str);
        } else {
            return "null";
        }
    }

    public int evaluate(String date_str, String flag) {
        if (flag.equals("count") && ! this.match_date(date_str).equals("null")) {
            date_str = this.match_date(date_str);
            return festivalMap.get(date_str) == null ? 0 :1;
        } else {
            return 0;
        }
    }

    public String match_date(String date_str) {
        //匹配20160101这种日期格式
        Pattern pat_common = Pattern.compile("\\d{8}");
        Matcher mat_common = pat_common.matcher(date_str);

        //匹配2016-01-01这种日期格式
        Pattern pat_strike = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");
        Matcher mat_strike = pat_strike.matcher(date_str);

        //匹配 2016-01-01 10:35:46 这种日期格式
        Pattern pat_colon = Pattern.compile("\\d{4}-\\d{2}-\\d{2}(\\s)+\\d{2}:\\d{2}:\\d{2}");
        Matcher mat_colon = pat_colon.matcher(date_str);

        if (mat_colon.find()) {
            return date_str.replace("-", "").substring(0, 8);
        } else if(mat_strike.find()) {
            return date_str.replace("-", "");
        } else if (mat_common.find()) {
            return date_str;
        } else {
            return "null";
        }
    }

    //测试的main方法
    public static void main(String[] args) throws Exception{
        FestivalType fes = new FestivalType();
        String date_str = "20150101";
        System.out.println(fes.evaluate(date_str));

        String date_str1 = "20160101";
        String result = fes.evaluate(date_str1);
        System.out.println("result is:" + result);

        double date_dou = 20160101;
        int result_dou = fes.evaluate(date_dou,"count");
        System.out.println(result_dou);
        System.out.println(date_dou);
    }
}
