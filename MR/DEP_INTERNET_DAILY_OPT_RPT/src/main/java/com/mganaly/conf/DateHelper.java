package com.mganaly.conf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateHelper {
	
	public static int daysBetween(String smdate,String bdate) throws ParseException{ 
		        
        SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");  
        Calendar cal = Calendar.getInstance();    
        cal.setTime(sdf.parse(smdate));    
        long time1 = cal.getTimeInMillis();                 
        cal.setTime(sdf.parse(bdate));    
        long time2 = cal.getTimeInMillis();         
        long between_days=(time2-time1)/(1000*3600*24);  
            
       return Integer.parseInt(String.valueOf(between_days));     
    }
	
	public static String getFMTyyyyMMdd (String inDate) throws ParseException {
		Date DateS = MGDateFormat.FORMAT_TILL_DATE.parse(inDate);
		return MGDateFormat.FORMAT_TILL_DATE_C.format(DateS);		
	}

}
