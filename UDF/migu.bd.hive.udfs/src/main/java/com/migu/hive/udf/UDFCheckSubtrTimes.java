package com.migu.hive.udf;


import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;

/**
 * Return the validity for a given substring times in string.
 * true is valid. false is invalid.
 *
 */
@Description(name = "UDFCheckSubtrTimes",
             value = "_FUNC_(string, substring, counter) UDFCheckSubtrTimes validity substring times in string",
            		 extended = "Example:\n"
            	             + "  > SELECT UDFCheckPositiveNumber(aaa, a, 3) FROM strings; "
            	             + "which will return true\n")

public class UDFCheckSubtrTimes extends UDF {
	
    public boolean evaluate(String value, String subVal, Integer times) throws UDFArgumentException {
    	
    	if(null == value || null == subVal || times < 0)
			return false;
		
    	int start_idx = 0;
		int end_idx = 0;
		int counter = 0;
		while (-1 != (end_idx = value.indexOf(subVal, start_idx))) {
			counter++;
			start_idx = end_idx + 1;
		}
		
		return times==counter;
    }
}
