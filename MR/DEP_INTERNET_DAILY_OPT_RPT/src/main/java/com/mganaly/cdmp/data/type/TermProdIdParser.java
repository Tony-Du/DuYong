package com.mganaly.cdmp.data.type;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 
 * @author xxx
 * 
 * class TermProdIdParser
 *
 */
public abstract class TermProdIdParser extends ColParser {


	public TermProdIdParser(String name) {
		super (name);
	}

	public eInvalidCounters getCounterId() {
		return eInvalidCounters.INV_TERM_PROV_ID;
	}
	
	protected boolean isDataClean(String strVal) {

		Pattern pattern = Pattern.compile("^P([0-9]{7})$");
		Matcher isValidate = pattern.matcher(strVal);

		return isValidate.matches();
	}

} // public static class TermProdIdParser