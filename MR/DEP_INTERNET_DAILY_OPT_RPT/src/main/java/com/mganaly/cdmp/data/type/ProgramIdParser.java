package com.mganaly.cdmp.data.type;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.mapreduce.Mapper.Context;


/**
 * 
 * @author xxx
 * 
 * abstract class ProgramIdParser
 *
 */
public abstract class ProgramIdParser extends DecimalParser {

	public ProgramIdParser(String name) {
		super(name);
	}
	

	public eInvalidCounters getCounterId() {
		return eInvalidCounters.INV_PROGRAM_ID;
	}

	protected boolean isDataClean(String strVal) {

		return isInRange(strVal, 6, 12);
	}

} // public static class ProgramIdParser