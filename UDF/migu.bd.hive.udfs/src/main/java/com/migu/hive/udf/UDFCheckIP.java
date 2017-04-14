package com.migu.hive.udf;


import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Return the validity for a given IP address.
 * true is valid. false is invalid.
 *
 */
@Description(name = "UDFCheckIP",
             value = "_FUNC_(IP) UDFCheckCdnId validity for a given IP address.\n")

public class UDFCheckIP extends UDF {
	

    public boolean evaluate(String IP) throws UDFArgumentException {

    	if(null == IP || IP.isEmpty())
			return false;
		
		Pattern pattern = Pattern.compile("^([0-9]{1,3}\\.){3}[0-9]{1,3}$"); 
		Matcher is_ip = pattern.matcher(IP.trim());
		return is_ip.matches();
    }
}




