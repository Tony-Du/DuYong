package com.mganaly.cdmp.data.type;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.mganaly.conf.GlobalEv;


/**
 * 
 * @author xxx
 * 
 * 
 * abstract class UseFlowParser
 *
 */
public abstract class FloatParser extends ColParser {
	protected long _float = 0;

	public FloatParser(String name) {
		super(name);
		_float = 0;
	}


	public eInvalidCounters getCounterId() {
		return eInvalidCounters.INV_FLOAT;
	}
	
	protected boolean isDataClean (String strVal) {
		
		Pattern pattern = Pattern.compile("^[0-9]{1,30}.[E0-9]{1,20}$");
		Matcher isFloat = pattern.matcher(strVal);

		return isFloat.matches();
		
	}

	public String toString() {
		return Long.toString(_float);
	}

	public boolean inputVal(String strVal) {

		boolean ret = super.inputVal(strVal);
		
		if (ret) {
			
			if (isDataClean(strVal)) {	
				_float = (long) Float.parseFloat(strVal);
				ret = true;
			}
			else if (GlobalEv.isNumeric(strVal)) {
				_float = Long.parseLong(strVal);
				ret = true;
			}
			
			else {
				_counter.increment(1);
				ret = false;
			}
		}

		return ret;
	}

	public void clear() {
		_float = 0;
	}
} // public static class UseFlowParser