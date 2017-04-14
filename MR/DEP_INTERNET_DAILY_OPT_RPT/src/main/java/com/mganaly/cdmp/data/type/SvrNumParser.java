package com.mganaly.cdmp.data.type;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.mapreduce.Mapper.Context;


/**
 * 
 * @author xxx
 * 
 * 
 * abstract class SvrNumParser
 *
 */

public abstract class SvrNumParser extends ColParser {

	public SvrNumParser(String name) {
		super(name);
	}

	public eInvalidCounters getCounterId() {
		return eInvalidCounters.INV_SERV_NUMBER;
	}
	
	protected boolean isDataClean(String strVal) {

		Pattern pattern = Pattern.compile("^[0-9]{6,20}$");
		Matcher isValidate = pattern.matcher(strVal);

		return isValidate.matches();
	}

} // public static class SvrNumParser
