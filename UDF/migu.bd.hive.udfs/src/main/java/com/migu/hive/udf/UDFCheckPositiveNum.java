package com.migu.hive.udf;


import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Return the validity for a given positive number.
 * true is valid. false is invalid.
 *
 */
@Description(name = "UDFCheckPositiveNum",
             value = "_FUNC_(positive_num ) - validity for a given positive number",
             extended = "Example:\n"
             + "  > SELECT UDFCheckPositiveNumber(positive_num) FROM nums;\n")
public class UDFCheckPositiveNum extends UDF {
	

    public boolean evaluate(String pos_num) throws UDFArgumentException {

    	if(null == pos_num || pos_num.isEmpty())
			return false;
		
		Pattern pattern = Pattern.compile("^[0-9]+(.[0-9Ee]{1,})?$"); 
		Matcher is_positive_num = pattern.matcher(pos_num.trim());
		return is_positive_num.matches();
    }
}
