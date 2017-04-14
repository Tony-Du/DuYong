package com.mganaly.cdmp.data.type;

public class StringParser extends ColParser {


	public StringParser(String finalVal) {
		super(finalVal);

		_colVal = finalVal;
	}

	public eInvalidCounters getCounterId() {
		return eInvalidCounters.INV_StringParser;
	}
	
	public boolean inputVal(String[] strVals) {
		return true;
	}

	@Override
	protected boolean isDataClean(String data) {
		// TODO Auto-generated method stub
		return true;
	}

}
