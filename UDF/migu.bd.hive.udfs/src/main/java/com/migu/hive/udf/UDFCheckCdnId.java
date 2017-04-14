package com.migu.hive.udf;


import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Return the validity for a given cdn id.
 * true is valid. false is invalid.
 *
 */
@Description(name = "UDFCheckCdnId",
             value = "_FUNC_(cdn_id) UDFCheckCdnId validity for a given CDN ID.\n")
public class UDFCheckCdnId extends UDF {
	
    public boolean evaluate(String cdn_id) throws UDFArgumentException {
    	
    	if(null == cdn_id || cdn_id.isEmpty())
			return false;
		
		Pattern pattern = Pattern.compile("^(?:(\\d{4}))$"); 
		Matcher is_cdn_id = pattern.matcher(cdn_id.trim());
		return is_cdn_id.matches();
    }
}
