package com.mganaly.cdmp.data.type;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.mganaly.conf.GlobalEv;

/**
 * 
 * @author xxx
 * 
 * class GateIpParser
 *
 */
public abstract class GateIpParser extends ColParser {

	
	private static final Log _LOG = LogFactory.getLog(GateIpParser.class);

	public GateIpParser(String name) {
		super(name);
		_colVal = "";
	}


	public eInvalidCounters getCounterId() {
		return eInvalidCounters.INV_GATE_IP;
	}

	protected boolean isDataClean (String strVal) {
		return GlobalEv.isIP(strVal);
	}
	
} // public static class GateIpParser