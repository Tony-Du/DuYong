package com.mganaly.cdmp.data.type;

public class ColNameParser extends ColParser {
	
	enum eColName {
		STATIS_DAY,
		CONTENT_ID,
		TERM_PROD_NAME
	}
		

	public ColNameParser(String name) {
		super(name);
	}

	public eInvalidCounters getCounterId() {
		return eInvalidCounters.INV_ColNameParser;
	}
	
	protected void formatOutput() {
		
		if (_defValType.equals(eDefaultType.IS_NOT_NULL) && isNull(_colVal)) {
			if (0 == _colName.compareToIgnoreCase(eColName.STATIS_DAY.name())) {
				_colVal = StatisDayParser.getDefaultVal();
			}
			else if (0 == _colName.compareToIgnoreCase(eColName.CONTENT_ID.name())) {
				_colVal = ContentIdParser.getDefaultVal();
			}
			else if (0 == _colName.compareToIgnoreCase(eColName.TERM_PROD_NAME.name())) {
				_colVal = "无记录";
			}
			else {
				_colVal = "unstatistic";
			}
		}
		else if (_defValType.equals(eDefaultType.IS_DEF_ZERO) && isNull(_colVal)) {
			_colVal = "0";			
		}
		else if (_defValType.equals(eDefaultType.NULL_AS_EMPTY) && isNull(_colVal)) {
			_colVal = "";			
		}
		else if (_colVal.isEmpty()) {
			_colVal = NULL;
		}
	}
	

	public String toString() {
		formatOutput();
		return _colVal;
	}

	@Override
	protected boolean isDataClean(String data) {
		if (!_cleanData.equals(eDataCleanStrict.NO_STRICT)) {
			return !isNull(data);
		}
		
		return true;
	}

}
