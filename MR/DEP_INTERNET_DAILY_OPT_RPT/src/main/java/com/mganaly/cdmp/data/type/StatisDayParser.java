package com.mganaly.cdmp.data.type;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.mganaly.conf.GlobalEv;

public abstract class StatisDayParser extends ColParser {

	public StatisDayParser(String name) {
		super(name);
	}

	public eInvalidCounters getCounterId() {
		return eInvalidCounters.INV_STATIS_DAY;
	}

	protected boolean isDataClean(String strVal) {
		Pattern pattern = Pattern.compile("^[0-9]{8}$");
		Matcher isValidate = pattern.matcher(strVal);
		
		return isValidate.matches();
	}
	
	public String getDefaultName () {
		return new String("STATIS_DAY");
	}


	public static String getDefaultVal () {
		String defaultDay = null;
		
		if (null != _conf) {
			defaultDay = _conf.get(DataParserCfg.MR_START_DAY);
		}
		
		if (null == defaultDay || GlobalEv.isNull(defaultDay)) {
			GlobalEv.getDay(-2);
		}
		
		return defaultDay;
	}

}
