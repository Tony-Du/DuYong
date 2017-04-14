package com.mganaly.cdmp.data.type;


public class ProductIdParser extends DecimalParser {
	

	public ProductIdParser(String name) {
		super(name);
	}


	public eInvalidCounters getCounterId() {
		return eInvalidCounters.INV_PRODUCT_ID;
	}

	protected boolean isDataClean(String strVal) {

		return isInRange(strVal, 8, 12);
	}
} //class PRODUCT_ID_Parser
