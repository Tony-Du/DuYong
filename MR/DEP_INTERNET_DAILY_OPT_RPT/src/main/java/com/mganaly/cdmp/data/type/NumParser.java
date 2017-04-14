package com.mganaly.cdmp.data.type;
/**
 * 
 * @author xxx
 *
 *	class NumParser 
 *
 */
public class NumParser extends ColParser {

	protected int num_val = 0;

	public NumParser(String idx) {
		super(idx);
	}

	public eInvalidCounters getCounterId() {
		return eInvalidCounters.INV_NULL;
	}

	protected boolean isDataClean (String strVal) {		
		return true;
	}

	public String toString() {
		num_val = Integer.parseInt(_colName);
		return String.valueOf(num_val);
	}

	public boolean inputVal(String strVal) {
		return true;
	}


	@Override
	public boolean inputVal(String[] strVals) {

		return true;
	}
	
} // public static class NumParser