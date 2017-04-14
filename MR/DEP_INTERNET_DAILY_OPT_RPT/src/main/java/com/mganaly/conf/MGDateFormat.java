package com.mganaly.conf;

import java.text.SimpleDateFormat;

public class MGDateFormat {
	
	public static SimpleDateFormat FORMAT_TILL_MIN_Z 		= new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
	public static SimpleDateFormat FORMAT_TILL_SEC_Z 		= new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	public static SimpleDateFormat FORMAT_TILL_HOUR_SLASH_C = new SimpleDateFormat("yyyyMMdd/HH");
	public static SimpleDateFormat FORMAT_TILL_HOUR_C 		= new SimpleDateFormat("yyyyMMddHH");
	public static SimpleDateFormat FORMAT_TILL_MONTH 		= new SimpleDateFormat("yyyy-MM");
	public static SimpleDateFormat FORMAT_TILL_MONTH_C 		= new SimpleDateFormat("yyyyMM");
	public static SimpleDateFormat FORMAT_TILL_DATE 		= new SimpleDateFormat("yyyy-MM-dd");
	public static SimpleDateFormat FORMAT_TILL_DATE_C 		= new SimpleDateFormat("yyyyMMdd");
	public static SimpleDateFormat FORMAT_TILL_HOUR 		= new SimpleDateFormat("yyyy-MM-dd/HH");
	public static SimpleDateFormat FORMAT_TILL_MIN 			= new SimpleDateFormat("yyyy-MM-dd/HH/mm");

}
