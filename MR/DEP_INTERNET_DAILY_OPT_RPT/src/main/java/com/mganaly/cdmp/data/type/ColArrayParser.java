package com.mganaly.cdmp.data.type;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * 
 * class ColArrayParser
 * 
 * SYNAX:
 * COLARR:startIndex(begin with 0)_endIndex
 *
 */
public class ColArrayParser extends ColParser {
	private static final Log _LOG = LogFactory.getLog(ColArrayParser.class);

	protected int		_start_idx;
	protected int		_end_idx;

	public ColArrayParser (String start_end_index) {
		super(start_end_index);
		
		String[] arrIdxs = start_end_index.trim().split("_");
		
		if (arrIdxs.length != 2) {

			_LOG.error("COLARR with parameter startIndex(begin with 0)_endIndex"
					+ " instead of " + start_end_index);
			return;
		}
		_start_idx			= Integer.parseInt(arrIdxs[0]);
		_end_idx			= Integer.parseInt(arrIdxs[1]);
	}

	public eInvalidCounters getCounterId() {
		return eInvalidCounters.INV_ColArrayParser;
	}


	public boolean inputVal(String strVal) {
		_colVal = strVal;
		return true;
	}

	@Override
	public boolean inputVal(String[] strVals) {

		if (strVals.length <= _end_idx) {
			_counter.increment(1);
			return false;
		}
		
		_colVal = "";
		for (int i = _start_idx; i <= _end_idx; ++i) {
			_colVal += strVals[i] + SEPRATOR;
		}
		
		if (!_colVal.isEmpty()) {
			_colVal = _colVal.substring(0, _colVal.length() - 1);
		}

		return true;
	}

	@Override
	protected boolean isDataClean(String data) {
		return true;
	}

}
