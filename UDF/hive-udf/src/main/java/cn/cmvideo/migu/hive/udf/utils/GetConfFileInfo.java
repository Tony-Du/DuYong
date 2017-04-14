package cn.cmvideo.migu.hive.udf.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

/**
 * Created by Smart on 2017/3/15.
 *
 * 用于读取hiveudf配置文件
 *
 */
public class GetConfFileInfo {

    private static HashMap<String, String> confInfo = new HashMap<>();

    static {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream("hiveudf.conf")));
            String tempLine;
            while (null != (tempLine = br.readLine())) {
                if(tempLine.startsWith("#")){
                    continue;
                }
                String[] fileds = tempLine.split("=");
                if(fileds.length==2){
                    confInfo.put(fileds[0].trim(), fileds[1].trim());
                }
            }
        } catch (IOException e) {
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                }
            }
        }
    }

    private GetConfFileInfo() {
    }

    /**
     * @param confName
     * @param defaultConfValue 未读取到配置时使用的默认配置
     * @return
     */
    public static String getConf(String confName, String defaultConfValue) {
        if(confInfo.containsKey(confName)){
            return confInfo.get(confName);
        } else {
            return defaultConfValue;
        }
    }

    public static String getConf(String confName) {
        return getConf(confName, "");
    }

    public static HashMap<String, String> getConfInfo() {
        return confInfo;
    }

}
