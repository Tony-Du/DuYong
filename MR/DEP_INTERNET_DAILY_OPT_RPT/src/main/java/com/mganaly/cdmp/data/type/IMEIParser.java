package com.mganaly.cdmp.data.type;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper.Context;
import com.mganaly.conf.GlobalEv;


/**
 * 
 * @author xxx
 * 
 * class IMEIParser
 *
 */
public abstract class IMEIParser extends ColParser {
	private static final Log _LOG = LogFactory.getLog(IMEIParser.class);
	

	public IMEIParser(String name) {
		super(name);
	}
	
	public eInvalidCounters getCounterId() {
		return eInvalidCounters.INV_IMEI_NEW;
	}

	protected boolean isDataClean (String strVal) {

		return GlobalEv.isIMEI(strVal);
		
	}


} // public static class GateIpParser