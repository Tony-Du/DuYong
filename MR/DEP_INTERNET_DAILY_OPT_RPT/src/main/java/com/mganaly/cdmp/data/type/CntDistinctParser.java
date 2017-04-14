package com.mganaly.cdmp.data.type;

import java.util.HashMap;


/**
 * 
 * @author xxx
 * 
 * class SumParser
 * 
 *
 */
public class CntDistinctParser extends ColParser {
	private HashMap<String, Integer> map_cnt = new HashMap<String, Integer>();

	public CntDistinctParser(String name) {
		super(name);
	}

	public eInvalidCounters getCounterId() {
		return eInvalidCounters.INV_CntDistinctParser;
	}


	public String toString() {
		return String.valueOf(map_cnt.size());
	}

	public boolean inputVal(String strVal) {
		
		boolean bSucceed = super.inputVal(strVal);
		
		if (bSucceed) {
			map_cnt.put(_colVal, 1);
		}
		
		return true;
	}

	public void clear() {
		map_cnt.clear();
	}

	@Override
	protected boolean isDataClean(String data) {
		return !isNull(data);
	}

} // public static class SumParser