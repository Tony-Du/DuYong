package com.mganaly.cdmp.data.type;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 
 * @author xxx
 * 
 * class SumParser
 *
 */
public class SumParser extends ColParser {

	protected long _cnt_val = 0;


	public SumParser(String name) {
		super(name);
		_cnt_val = 0;
	}

	public eInvalidCounters getCounterId() {
		return eInvalidCounters.INV_SumParser;
	}
	
	protected boolean isDataClean(String strVal) {
		
		Pattern pattern = Pattern.compile("^[0-9]{1,18}$");
		Matcher isValid = pattern.matcher(strVal);
		return isValid.matches();
		
	}

	public String toString() {
		return Long.toString(_cnt_val);
	}

	public boolean inputVal(String strVal) {
		
		boolean bSucceed = isDataClean(strVal);
		if (bSucceed) {
			_cnt_val += Long.parseLong(strVal);
		}
		else {
			_counter.increment(1);
		}
		
		return true;
	}

	public void clear() {
		_cnt_val = 0;
	}
	
} // public static class CntParser
