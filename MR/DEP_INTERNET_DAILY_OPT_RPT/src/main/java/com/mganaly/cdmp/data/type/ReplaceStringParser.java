package com.mganaly.cdmp.data.type;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * class ReplaceStringParser
 * 
 * SYNAX:
 * REPLACE:replaceIndex_replaceString_RegularExpressIndex[OPT]_RegularExpress[OPT]_times[OPT]
 *
 */
public class ReplaceStringParser extends ColParser {
	
	private static final Log _LOG = LogFactory.getLog(ReplaceStringParser.class);

	protected String 	_replaceString	= "";
	protected int 		_RegExp_index 	= 0;
	protected String 	_reg_exp		= "";
	protected long		_times			=-1;

	public ReplaceStringParser(String replace_exp) {
		super(replace_exp);
		
		String[] re = replace_exp.trim().split("_");
		
		if (re.length < 2) {

			_LOG.error("ReplaceStringParser_index_replaceString_replaceRegularExpress"
					+ " parameter instead of " + replace_exp);
			return;
		}

		_colIdx		= Integer.parseInt(re[0]);
		_replaceString	= re[1];
		_RegExp_index	= Integer.parseInt(re[2]);
		
		if (re.length >= 4) {
			_reg_exp	= re[3];
		}
		
		if (re.length >= 5) {
			_times = Long.parseLong(re[4]);
		}
	}

	public eInvalidCounters getCounterId() {
		return eInvalidCounters.INV_ReplaceStringParser;
	}

	protected boolean isDataClean(String strVal) {

		Pattern pattern = Pattern.compile(_reg_exp);
		Matcher isMatch = pattern.matcher(strVal);

		return isMatch.matches();
	}


	public boolean inputVal(String strVal) {
		

		if (0 != _times && super.inputVal(strVal)) {
			_colVal = _replaceString;
			_times --;
		}
		else {
			_colVal = strVal;
		}
		
		return true;
	}

	@Override
	public boolean inputVal(String[] strVals) {
		
		if (_colIdx >= strVals.length || _RegExp_index >= strVals.length) {
			_counter.increment(1);
			_LOG.error("inputVal: String[] strVals length " + strVals.length 
					+ "less tha index " + _colIdx);
			return false;
		}
		
		if (0 != _times && super.inputVal(strVals[_RegExp_index])) {
			_colVal = _replaceString;
			_times --;
		}
		else {
			_colVal = strVals[_colIdx];			
		}
		return true;
	}

}
