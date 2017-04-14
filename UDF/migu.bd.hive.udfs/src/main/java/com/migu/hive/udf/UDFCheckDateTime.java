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
@Description(name = "CheckUDFDateTime",
             value = "_FUNC_(YYYYMMDDHH24MiSS[.mis] or MMDDYYYYHH24MiSS[.mis] or DDMMYYYYHH24MiSS[.mis]) "
             		+ "- Y, M, D suport [/-., ] seprator"
             		+ "- H, M , S support [:] seprator"
             		+ "- millisecond support [.] seprator\n")
public class UDFCheckDateTime extends UDF {
	

    public boolean evaluate(String dateTime) throws UDFArgumentException {
    	
    	if(null == dateTime || dateTime.isEmpty())
			return false;
		
		Pattern pattern = Pattern.compile("^(?:(?:(?:(?:[1-2][0-9]{3})(?:[\\/\\-\\., ]?)"
				+ "(?:1[0-2]|0?[1-9])(?:[\\/\\-\\., ]?)*(?:[12][0-9]|3[01]|0?[1-9]))|(?:(?:1[0-2]|0?[1-9])"
				+ "(?:[\\/\\-\\., ]?)(?:[12][0-9]|3[01]|0?[1-9])(?:[\\/\\-\\., ]?)(?:(?:[0-9]{1,2})|"
				+ "(?:[1-2][0-9]{3})))|(?:(?:[12][0-9]|3[01]|0?[1-9])(?:[\\/\\-\\., ]?)(?:1[0-2]|0?[1-9])"
				+ "(?:[\\/\\-\\., ]?)(?:(?:[0-9]{1,2})|(?:[1-2][0-9]{3}))))(\\s*)(?:(?:(?:1[0-2]|0?[1-9])"
				+ "(?:(?:\\:?)(?:[1-5][0-9]|0?[0-9]))(?:(?:\\:?)(?:[1-5][0-9]|0?[0-9]))((?:[ap]m)))|"
				+ "(?:(?:2[0-3]|[01]?[0-9])(?:(?:\\:?)(?:[1-5][0-9]|0?[0-9]))(?:(?:\\:?)(?:[1-5][0-9]|0?[0-9]))"
				+ "((?:([\\-\\/\\.]?[0-9]{3})?)))))$"); 
		Matcher isDateTime = pattern.matcher(dateTime.trim());
		return isDateTime.matches();
    }
}
