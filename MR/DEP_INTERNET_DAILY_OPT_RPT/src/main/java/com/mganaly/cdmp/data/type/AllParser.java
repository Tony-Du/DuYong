package com.mganaly.cdmp.data.type;



public class AllParser extends ColParser {


	public AllParser (String name) {
		super(name);
	}

	public eInvalidCounters getCounterId() {
		return eInvalidCounters.INV_AllParser;
	}
	
	@Override
	public boolean inputVal(String strVal) {		
		return false;
	}


	@Override
	public boolean inputVal(String[] strVals) {

		boolean bInput = true;
		
		if (strVals.length == 0) {
			_counter.increment(1);
			bInput = false;
		}
		
		if (bInput) {
			for (String val : strVals) {
				
				if (!super.inputVal(val)) {
					bInput = false;
					break;
				}
				_colVal += val + SEPRATOR;
			}
		}

		return bInput;
	}

	@Override
	protected boolean isDataClean(String data) {
		return true;
	}

}
