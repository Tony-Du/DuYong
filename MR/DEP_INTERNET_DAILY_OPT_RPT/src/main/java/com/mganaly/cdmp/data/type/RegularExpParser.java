package com.mganaly.cdmp.data.type;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * class RegularExpParser
 *
 */
public class RegularExpParser extends ColParser {
	

	protected String	_reg_exp;

	
	private static final Log _LOG = LogFactory.getLog(GateIpParser.class);

	public RegularExpParser(String reg_exp) {
		super(reg_exp);
		_reg_exp 	= reg_exp;
	}


	public eInvalidCounters getCounterId() {
		return eInvalidCounters.INV_RegularExpParser;
	}

	protected boolean isDataClean(String strVal) {

		Pattern pattern = Pattern.compile(_reg_exp);
		Matcher isMatch = pattern.matcher(strVal);

		return isMatch.matches();
	}
	
	public boolean inputVal(String[] strVals) {
		_colVal = "";
		int idx = 0;
		for (String val : strVals) {
			if (super.inputVal(val)) {
				_colVal += idx + "_" + val + SEPRATOR;		
			}
			idx++;
		}
		
		if (!_colVal.isEmpty()) {
			_colVal = _colVal.substring(0, _colVal.length() - 1);
		}
		
		return !(_colVal.isEmpty());
	}


}
