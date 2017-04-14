package com.mganaly.cdmp.data.type;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class DecimalParser extends ColParser {
	

	public DecimalParser(String name) {
		super(name);
	}
	
	public eInvalidCounters getCounterId() {
		return eInvalidCounters.INV_DecimalParser;
	}

	protected boolean isInRange(String decimalVal, int minLen, int maxLen) {

		Pattern pattern = Pattern.compile("^\\d{" + minLen+ "," + maxLen + "}$");
		Matcher isIn = pattern.matcher(decimalVal.trim());			
		return isIn.matches();
		
	}
	
	protected boolean isInRange(String decimalVal, int minLen) {

		Pattern pattern = Pattern.compile("^\\d{" + minLen+ ", }$");
		Matcher isIn = pattern.matcher(decimalVal.trim());
		return isIn.matches();
		
	}
	
	protected boolean isInBits(String decimalVal, int bits) {

		Pattern pattern = Pattern.compile("^\\d{" + bits+ "}$");
		Matcher isIn = pattern.matcher(decimalVal.trim());
		return isIn.matches();		
	}
}