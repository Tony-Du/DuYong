package com.mganaly.cdmp.data.type;

/**
 * 
 * @author xxx
 * 
 * abstract class TermTypeParser
 *
 */
public abstract class TermTypeParser extends ColParser {

	public TermTypeParser(String name) {
		super(name);
	}
	

	public eInvalidCounters getCounterId() {
		return eInvalidCounters.INV_TERM_TYPE;
	}

	protected boolean isDataClean(String strVal) {

		boolean bValid = false;
		if (strVal.contains("_")) {
			bValid = true;

			String[] vals = strVal.trim().split("_");

			for (String val : vals) {
				if (val.isEmpty()) {
					bValid = false;
					break;
				}
			}
		} else if (strVal.contains("Android")) {
			bValid = true;
		}

		return bValid;
		
	}


} // public static class TermTypeParser
