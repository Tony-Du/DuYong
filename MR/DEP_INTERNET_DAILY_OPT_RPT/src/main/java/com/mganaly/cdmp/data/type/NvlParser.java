package com.mganaly.cdmp.data.type;

import java.io.IOException;

public class NvlParser extends ColParser {
	
	private int _left_idx = -1;
	private int _right_idx = -1;

	public NvlParser(String nvlValues) {
		super(nvlValues.trim().split("_")[0]);
		
		String[] nvls = nvlValues.trim().split("_");
		
		_left_idx = Integer.parseInt(nvls[0]);
		_right_idx = Integer.parseInt(nvls[1]);
	}

	public eInvalidCounters getCounterId() {
		return eInvalidCounters.INV_NvlParser;
	}


	public String toString() {
		return _colVal;
	}

	public boolean inputVal(String[] strVals) throws IOException {
		
		int idx = getIndex();

		if (idx >= strVals.length || idx < 0) {
			dumpErr(" input vals length is " + strVals.length);
		}
		
		if (!isNull(strVals[_left_idx])) {
			_colVal = strVals[_left_idx];
		}
		else {
			_colVal = strVals[_right_idx];
		}
		
		return true;
	}

	public void clear() {
		_colVal = "";
	}

	@Override
	protected boolean isDataClean(String data) {
		return true;
	}

}
