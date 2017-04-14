package com.mganaly.cdmp.data.type;


/**
 * 
 * @author xxx
 * 
 * class ColIdxParser
 *
 */
public class ColIdxParser extends ColParser {

	public ColIdxParser(String str_idx) {
		super(str_idx);
		_colIdx = Integer.parseInt(str_idx);
	}


	public eInvalidCounters getCounterId() {
		return eInvalidCounters.INV_ColIdxParser;
	}


	@Override
	protected boolean isDataClean(String data) {
		// TODO Auto-generated method stub
		return true;
	}
	
} // public static class ColIdxParser