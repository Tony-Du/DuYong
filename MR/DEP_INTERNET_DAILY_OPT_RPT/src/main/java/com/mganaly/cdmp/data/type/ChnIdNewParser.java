package com.mganaly.cdmp.data.type;

import com.mganaly.conf.GlobalEv;


/**
 * 
 * @author xxx
 * 
 * abstract class ChnIdNewParser
 *
 */
public abstract class ChnIdNewParser extends ColParser {

	public ChnIdNewParser(String name) {
		super(name);
	}

	public eInvalidCounters getCounterId() {
		return eInvalidCounters.INV_CHN_ID_NEW;
	}


	protected boolean isDataClean(String channel) {
		
		return GlobalEv.isChannelNew(channel);
	}


} // public static class ChnIdNewParser