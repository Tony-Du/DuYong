package com.iflytek.gnome.analysis.mobile.dsp;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.fs.Path;

import com.iflytek.share.util.ConstantsUtils;
import com.iflytek.share.util.LogParseKey;
import com.iflytek.share.util.ShareConstants;
import com.iflytek.source.log.msp.MspDataSource;

public class DspSidParse {
  
  private static final int SESSION_ID_LEN = 32;
  private static final String BASE_TIME = "2010-10-01/00";
  private static final long BASE_MS_TIME = 1285862400000L;
  private static final String SID_REGEX = "^[a-zA-Z]{3}[0-9a-zA-Z]{8}@[a-zA-Z]{2}[0-9a-zA-Z]{4}[0-9a-fA-F]{12}[0-9a-zA-Z]{2}$";
  private static final String ENGIP_SID_REGEX = "^[a-zA-Z]{3}[0-9a-zA-Z]{8}@[a-zA-Z]{2}[0-9a-fA-F]{16}[0-9a-zA-Z]{2}$";
  private static final Pattern pattern = Pattern.compile(SID_REGEX);
  private static final Pattern engipPattern = Pattern.compile(ENGIP_SID_REGEX);
  private static final String HF = "hdfs://hfa-pro0002.hadoop.cpcc.iflyyun.cn:8020";
  private static final String GZ = "hdfs://gza-pro0002.hadoop.cpcc.iflyyun.cn:8020";
  private static final String BJ = "hdfs://bja-pro0002.hadoop.cpcc.iflyyun.cn:8020";
  
  public static boolean isValidSid(String sid) {
    if (sid == null || sid.length() != SESSION_ID_LEN) {
      return false;
    }
    
    Matcher matcher = pattern.matcher(sid);
    return matcher.find();
  }
  
  public static boolean isEngIpSid(String sid) {
    if (sid == null || sid.length() != SESSION_ID_LEN) {
      return false;
    }
    
    Matcher matcher = engipPattern.matcher(sid);
    return matcher.find();
  }
  
  public static String sidToDate(String sid, SimpleDateFormat simpleFormat) {
    /***
     * 瀵逛簬鏃堕棿瑙ｆ瀽锛屼笉鍋歴id寮傚父鍒ゆ柇锛岀幇鍦╯id鍩烘湰鍙互淇濊瘉鏃堕棿閮芥湁鏁�濡傛灉鐪熺殑鏃犳硶瑙ｆ瀽sid涓殑鏃堕棿锛屽湪鏃堕棿瑙ｆ瀽澶辫触鏃舵崟鑾峰紓甯稿嵆鍙�
     */
    if (null == sid || sid.length() != SESSION_ID_LEN) return null;
    
    try {
      //long baseTime = ShareConstants.FORMAT_OUTPUT.parse(BASE_TIME).getTime();
      long baseTime = BASE_MS_TIME;
      long sidTime = baseTime + 1000
          * Long.parseLong(sid.substring(18, 26), 16);
      return simpleFormat.format(new Date(sidTime)).toString();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }
  
  public static Date sidToDate(String sid) {
    /***
     * 瀵逛簬鏃堕棿瑙ｆ瀽锛屼笉鍋歴id寮傚父鍒ゆ柇锛岀幇鍦╯id鍩烘湰鍙互淇濊瘉鏃堕棿閮芥湁鏁�濡傛灉鐪熺殑鏃犳硶瑙ｆ瀽sid涓殑鏃堕棿锛屽湪鏃堕棿瑙ｆ瀽澶辫触鏃舵崟鑾峰紓甯稿嵆鍙�
     */
    // if (SidParse.isValidSid(sid) == false) return null;
    if (null == sid || sid.length() != SESSION_ID_LEN) return null;
    
    try {
      //long baseTime = ShareConstants.FORMAT_OUTPUT.parse(BASE_TIME).getTime();
      long baseTime = BASE_MS_TIME;
      long sidTime = baseTime + 1000
          * Long.parseLong(sid.substring(18, 26), 16);
      return new Date(sidTime);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }
  
  public static long sidToDay(String sid) {
    String date = DspSidParse.sidToDate(sid, ShareConstants.FORMAT_OUTPUT);
    if (date == null) return -1;
    
    date = date.substring(0, 10) + "/00";
    try {
      long time = ShareConstants.FORMAT_OUTPUT.parse(date).getTime();
      return time;
    } catch (ParseException e) {
      e.printStackTrace();
    }
    
    return -1;
  }
  
  public static long sidToHour(String sid) {
    String date = DspSidParse.sidToDate(sid, ShareConstants.FORMAT_OUTPUT);
    if (date == null) return -1;
    
    date = date.substring(11, 13);
    if (date.startsWith("0")) return Long.parseLong(date.substring(1));
    else return Long.parseLong(date);
  }
  
  public static long sidToTimestamp(String sid) {
    /***
     * 瀵逛簬鏃堕棿瑙ｆ瀽锛屼笉鍋歴id寮傚父鍒ゆ柇锛岀幇鍦╯id鍩烘湰鍙互淇濊瘉鏃堕棿閮芥湁鏁�濡傛灉鐪熺殑鏃犳硶瑙ｆ瀽sid涓殑鏃堕棿锛屽湪鏃堕棿瑙ｆ瀽澶辫触鏃舵崟鑾峰紓甯稿嵆鍙�
     */
    // if (SidParse.isValidSid(sid) == false) return -1;
    if (null == sid || sid.length() != SESSION_ID_LEN) return -1;
    try {
      //long baseTime = ShareConstants.FORMAT_OUTPUT.parse(BASE_TIME).getTime();
      long baseTime = BASE_MS_TIME;
      long sidTime = 1000* Long.parseLong(sid.substring(18, 26), 16) + baseTime;
      return sidTime;
    } catch (NumberFormatException e) {
      e.printStackTrace();
    }
    
    return -1;
  }
  
  public static String sidToSub(String sid) {
    if (DspSidParse.isValidSid(sid) == false) return null;
    
    return sid.substring(0, 3);
  }
  
  public static String sidToMssIp(String sid) {
    if (DspSidParse.isValidSid(sid) == false) return null;
    
    return LogParseKey.IP_PREFIX
        + String.valueOf(Integer.parseInt(sid.substring(26, 28), 16)) + "."
        + String.valueOf(Integer.parseInt(sid.substring(28, 30), 16));
  }
  
  public static String sidToEngIp(String sid) {
    if (DspSidParse.isEngIpSid(sid) == false) return null;
    
    return LogParseKey.IP_PREFIX
        + String.valueOf(Integer.parseInt(sid.substring(14, 16), 16)) + "."
        + String.valueOf(Integer.parseInt(sid.substring(16, 18), 16));
  }
  
  public static String sidToArea(String sid) {
    if (DspSidParse.isValidSid(sid) == false) return null;
    
    return sid.substring(12, 14);
    
  }
  
  public static String sidGenerate(String sub, int mapID, int number, long ms) {
	
    String sid = sub;
    sid += String.format("%08d", number%100000000);
    sid += "@hf0000";
    sid += String.format("%08x", ms / 1000 - 1285862400L);
    sid += String.format("%06d", mapID%1000000);
    return sid;
  }
  
  public static String mrSidGenerate(String sub, int number, long ms,String ip, int taskID) {
    String sid = "nil";
    int ip1 = 0;
    int ip2 = 0;
    if (sub.length() == 3) sid = sub;
    String IP_REGEX = "^\\d{1,3}.\\d{1,3}.(\\d{1,3}).(\\d{1,3})$";
    Pattern ipPattern = Pattern.compile(IP_REGEX);
    Matcher matcher = ipPattern.matcher(ip);
    if(matcher.find()){
      String ipStr1 = matcher.group(1);
      String ipStr2 = matcher.group(2);
      ip1 = Integer.valueOf(ipStr1);
      ip2 = Integer.valueOf(ipStr2);
    }else{
      return null;
    }
    
    sid += String.format("%08d", number);
    sid += String.format("@hf%02x", ip1);
    sid += String.format("%02x", ip2);
    sid += String.format("%08x", ms / 1000 - 1285862400L);
    sid += String.format("%06d", taskID % 1000000);
    return sid;
  
    
  }
  
  public static String Sid2BaseStorePath(String sid) {
    String re = null;
    if (sid.indexOf("@hf") > 0) {
      re = HF;
    } else if (sid.indexOf("@gz") > 0) {
      re = GZ;
    } else if (sid.indexOf("@ch") > 0) {
      re = BJ;
    }
    return re;
  }
  
  public static String getHfBasePath() {
    return HF;
  }
  
  public static String getGzBasePath() {
    return GZ;
  }
  
  public static String getBjBasePath() {
    return BJ;
  }
  
  
  public static Set<Path> getSidPaths(List<String>sids){
	  if(CollectionUtils.isEmpty(sids)){
		  return null;
	  }
	  for(String sid:sids){
		 sidToDate(sid);	
	  }
	  return null;
	  
  }
  
  public static void main(String[] args) {


    String[] sidArray = {"iat00012f29@hf3eee06958346481c00"};

    for (String sid : sidArray) {
      System.out.println(sid + " is valid sid: \t\t\t" + isValidSid(sid));
      System.out.println(sid + " is sid content eng_ip: \t"
          + DspSidParse.isEngIpSid(sid));
      System.out.println(sid + " sub: \t\t" + DspSidParse.sidToSub(sid));
      System.out.println(sid + " s_city: \t" + DspSidParse.sidToArea(sid));
      System.out.println(sid + " eng_ip: \t" + DspSidParse.sidToEngIp(sid));
      System.out.println(sid + " date: \t\t" + DspSidParse.sidToDate(sid));
      System.out.println(sid + " time: \t\t" + DspSidParse.sidToTimestamp(sid));
      System.out.println(sid + " mss_ip: \t" + DspSidParse.sidToMssIp(sid));
      System.out.println(sid + " mss_ip: \t" + DspSidParse.sidToMssIp(sid));
      System.out.println( ConstantsUtils
    	      .getCurrentDir(MspDataSource.get().getOutput(DspSidParse.sidToDate(sid))));
      
    }
  }
}
