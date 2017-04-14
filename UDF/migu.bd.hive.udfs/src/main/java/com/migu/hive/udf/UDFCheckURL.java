package com.migu.hive.udf;


import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Return the validity for a given date & time.
 * true is valid. false is invalid.
 *
 */
@Description(name = "UDFCheckURL",
             value = "_FUNC_(YYYYMMDDHH24MiSS[.mis] or MMDDYYYYHH24MiSS[.mis] or DDMMYYYYHH24MiSS[.mis]) "
             		+ "- Y, M, D suport [/-., ] seprator"
             		+ "- H, M , S support [:] seprator"
             		+ "- millisecond support [.] seprator\n")
public class UDFCheckURL extends UDF {	
	
    public boolean evaluate(String url) throws UDFArgumentException {
    	
    	if(null == url || url.isEmpty())
			return false;
		
    	Pattern pattern = Pattern.compile("^(.*?)(?:((?:https?|ftp):\\/\\/)?)(?:\\S+(?::\\S*)?@)?(?:(?!(?:10|127)(?:\\.\\d{1,3}){3})(?!(?:169\\.254|192\\.168)(?:\\.\\d{1,3}){2})(?!172\\.(?:1[6-9]|2\\d|3[0-1])(?:\\.\\d{1,3}){2})(?:[1-9]\\d?|1\\d\\d|2[01]\\d|22[0-3])(?:\\.(?:1?\\d{1,2}|2[0-4]\\d|25[0-5])){2}(?:\\.(?:[1-9]\\d?|1\\d\\d|2[0-4]\\d|25[0-4]))|(?:(?:[a-z\\u00a1-\\uffff0-9]-*)*[a-z\\u00a1-\\uffff0-9]+)(?:\\.(?:[a-z\\u00a1-\\uffff0-9]-*)*[a-z\\u00a1-\\uffff0-9]+)*(?:\\.(?:[a-z\\u00a1-\\uffff]{2,}))\\.?)(?:\\:\\d{2,5})?(?:[\\/\\?\\#]\\S*)[\\W\\w]*$"); 
		Matcher isDateTime = pattern.matcher(url.trim());
		
		return isDateTime.matches();
    }
    
    
    
}
