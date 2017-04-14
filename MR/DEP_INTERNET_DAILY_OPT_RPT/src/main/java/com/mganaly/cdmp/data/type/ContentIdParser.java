package com.mganaly.cdmp.data.type;


/*
 * class ContentIdParser
 */
public abstract class ContentIdParser extends DecimalParser {
	
	public ContentIdParser(String name) {
		super(name);
	}

	public eInvalidCounters getCounterId() {
		return eInvalidCounters.INV_CONTENT_ID;
	}
	

	protected boolean isDataClean(String strVal) {		
		return isInRange(strVal, 8, 12);
	}


	public static String getDefaultVal () {
		return new String("0000000000");
	}
}
