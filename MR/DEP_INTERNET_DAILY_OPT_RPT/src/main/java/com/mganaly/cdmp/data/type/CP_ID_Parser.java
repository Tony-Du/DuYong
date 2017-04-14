package com.mganaly.cdmp.data.type;

public class CP_ID_Parser extends DecimalParser {
	

	public CP_ID_Parser(String name) {
		super(name);
	}


	public eInvalidCounters getCounterId() {
		return eInvalidCounters.INV_CP_ID;
	}

	protected boolean isDataClean(String decimalVal) {
		
		return isInRange(decimalVal, 4, 6);
	}
}
