package com.mganaly.cdmp.data.type;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class PageIdParser extends ColParser {
	
	private static final Log _LOG = LogFactory.getLog(PageIdParser.class);


	public PageIdParser(String name) {
		super(name);
	}

	@Override
	public eInvalidCounters getCounterId() {
		return eInvalidCounters.INV_PAGE_ID;
	}

	protected boolean isDataClean(String strVal) {
		Pattern pattern = Pattern.compile("^[0-9]{8}$");
		Matcher isValidate = pattern.matcher(strVal);

		return isValidate.matches();
	}

}
