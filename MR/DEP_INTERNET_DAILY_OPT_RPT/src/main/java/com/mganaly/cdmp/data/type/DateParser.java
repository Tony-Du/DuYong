package com.mganaly.cdmp.data.type;

import java.io.IOException;


public class DateParser extends ColParser {
	
	private String _Date = null;

	public DateParser(String colDef) {
		super(colDef);
		
	}
	
	public eInvalidCounters getCounterId() {
		return eInvalidCounters.INV_DateParser;
	}

	protected boolean isDataClean(String strVal) {
		return true;
	}
	
	public boolean inputVal(String strVal) {
		_colInput = strVal;
		
		return true;
	}

	public boolean inputVal(String[] strVals) throws IOException {
		return true;
	}
	


	public String toString() {
		if (null == _Date) {
			readDate(_colName);
		}
		return _Date;
	}
	
	
	protected void readDate (String colDef) {
		if (0 == colDef.compareToIgnoreCase("startDate")) {
			_Date = _conf.get(DataParserCfg.MR_START_DAY);			
		}		
	}
}
