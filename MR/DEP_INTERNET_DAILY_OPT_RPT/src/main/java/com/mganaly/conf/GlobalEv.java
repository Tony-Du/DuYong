package com.mganaly.conf;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class GlobalEv {
	private static final Log _LOG = LogFactory.getLog(GlobalEv.class);


	public static boolean DEBUG = false; 
	
	public static int 	filterColExc 			= 1; //Cluster filter column numbers
	
	
	
	// Set reduce parameter
	public final static int REDUCE_NUM_ONE 		= 1;
	public final static int REDUCE_NUM_lOW 		= 6;
	public final static int REDUCE_NUM_NORMAL 	= 8;
	public final static int REDUCE_NUM_HIGH 		= 64;
	public final static int REDUCE_NUM_VERY_HIGH 	= 128;
	
	//MY SQL 
	public final static String mysqlJDBC = "com.mysql.jdbc.Driver";
	public final static String mysqlDB = "jdbc:mysql://10.200.63.80:3306/bigdata";
	public final static String mysqlUsr = "bi_read";
	public final static String mysqlPwd = "bi_read123";

	public final static String mysqlCntor ="/user/hadoop/public/migu_analyo_ly/bin/mysql-connector-java-5.1.16-bin.jar";
	
	// Functions
	public static boolean isNull(String strVal) {
		
		boolean bIsNull = false;
		
		if (strVal.isEmpty()) {
			bIsNull = true;
		}
		else {
			Pattern pattern = Pattern.compile("((null)*|(NULL)*|-*|\\s*)*");
			Matcher mIsNull = pattern.matcher(strVal);
			bIsNull = mIsNull.matches();
		}

		return bIsNull;
	}
	
	
	public static boolean isNumeric(String value){

		if(value.isEmpty())
			return false;
		
		Pattern pattern = Pattern.compile("[0-9]+"); 
		Matcher isNum = pattern.matcher(value);
		return isNum.matches();
	}
	
	public static boolean isIP(String value) {

		if(value.isEmpty())
			return false;
		
		Pattern pattern = Pattern.compile("\\b((?!\\d\\d\\d)\\d+|1\\d\\d"
				+ "|2[0-4]\\d|25[0-5])\\.((?!\\d\\d\\d)\\d+|1\\d\\d|2[0-4]\\d"
				+ "|25[0-5])\\.((?!\\d\\d\\d)\\d+|1\\d\\d|2[0-4]\\d|25[0-5])\\."
				+ "((?!\\d\\d\\d)\\d+|1\\d\\d|2[0-4]\\d|25[0-5])\\b");
		Matcher isIP = pattern.matcher(value);
		return isIP.matches();
	}
	
	public static boolean isIMEI(String value) {
		
		final int imei_min_len = 15;
		if(value.length() < imei_min_len)
			return false;

		Pattern pattern = Pattern.compile("^[A-Za-z0-9]{1,8}=[A-Za-z0-9]{2,20}$");
		Matcher isValidate = pattern.matcher(value);
		return isValidate.matches();
	}
	
	public static boolean IsTelNum(String value) {
		if(value.isEmpty())
			return false;
		
		Pattern pattern = Pattern.compile("^\\d{6,20}$");
		Matcher isTelNum = pattern.matcher(value);
		return isTelNum.matches();
	}
	
	public static boolean IsFloat(String value) {
		if(value.isEmpty())
			return false;
		
		Pattern pattern = Pattern.compile("^[0-9]{1,60}.[E0-9]{1,20}$");
		Matcher isFloat = pattern.matcher(value);
		return isFloat.matches();
	}
	
	public static boolean isChannelNew(String channel){
		
		if(channel.isEmpty())
			return false;
		
		if (channel.length() != 15 
				&& channel.length() != 11)
			return false;
		
		if (!isNumeric(channel))
			return false;
		
		return true;
	}
	
	public static boolean isChannelSrc(String channel){
			
			if(channel.isEmpty())
				return false;
			//0111_61040105-99000-800000380000000
			//[0-9]{1,6}_[0-9]{6,10}-[0-9]{3,7}-[0-9]{10,20}$
			Pattern pattern = Pattern.compile("[0-9]{2,8}_[0-9,_,-]*"); 
			Matcher isValidate = pattern.matcher(channel);
			return isValidate.matches();
		}

	public static void checkAndRemove(Configuration conf, String strpath) {
		try {
			FileSystem fs = FileSystem.get(conf);

			Path path = new Path(strpath);
			if (fs.exists(path)) {
				fs.delete(path, true);
				_LOG.info("Remove path >>>>>>>>>>" + path.toString());
				// TODO: BUG for oozie???
				//fs.close(); 

			}
			/*
			 * recursively delete the file(s) if it is adirectory. If you want
			 * to mark the path that will bedeleted as a result of closing the
			 * FileSystem. deleteOnExit(Path f)
			 */

		} catch (IOException e) {
			new RuntimeException(e);
		}
	}
	
	public String rmLastChar (String strValue) {
			return strValue.substring(0, strValue.length() - 1);
	}
	
	public static String getToday() {
		return getNowFmt(new String("yyyyMMdd"));
	}
	

	public static String getCurTime() {
		// counter time stamp
		return getNowFmt("yyyyMMddHHmmss");
	}

	

	public static String getNowFmt( String tmFormate) {
		// counter time stamp
		SimpleDateFormat df = new SimpleDateFormat(tmFormate);
		return df.format(new Date());
	}
	
	public static String getDay(int amout) {  
		Date date = new Date();
        Calendar calendar = Calendar.getInstance();  
        calendar.setTime(date);  
        calendar.add(Calendar.DAY_OF_MONTH, amout);  
        date = calendar.getTime(); 
        
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        return df.format(date); 
    }
	
}
