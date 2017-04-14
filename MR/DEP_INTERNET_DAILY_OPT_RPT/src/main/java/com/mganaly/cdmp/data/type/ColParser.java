package com.mganaly.cdmp.data.type;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;

public abstract class ColParser {
	
	private static final Log _LOG = LogFactory.getLog(ColParser.class);
	
	public enum eDefaultType {
		IS_NOT_NULL,
		IS_DEF_ZERO,
		NULL_AS_EMPTY,
		NO_DEF
	}
	
	public enum eDataCleanStrict {
		NOT_NULL,
		CUSTOME,
		NO_STRICT
	}

	protected String	_colName 		= null;
	protected String	_colVal			= null;

	protected String	_colInput		= null;
	protected int		_colIdx	 		= -1;
	protected Counter	_counter 		= null;
	protected eDefaultType _defValType		= eDefaultType.NO_DEF;
	protected eDataCleanStrict _cleanData 	= eDataCleanStrict.NO_STRICT;
	
	public final static String NULL						= "NULL";
	public static final String 	SEPRATOR 				= "\t";  // Tab
	public static final String 	SEPRATOR_SPLIT 			= "\\x09"; // Tab
	public static final String 	SEPRATOR_SPACE_SPLIT 	= "\\x20"; //SPACE

	protected static Configuration	_conf = null;
	
	
	public ColParser(String colDef) {
		checkRequire(colDef);	
		_colVal = "";
	}
	
	protected abstract boolean isDataClean(String data);

	private boolean isValidate(String data) {
		
		if ( _cleanData.equals(eDataCleanStrict.NO_STRICT)) {
			return true;
		}
		
		if (_cleanData.equals(eDataCleanStrict.CUSTOME)) {
			return isDataClean(data);
		}
		
		if (_cleanData.equals(eDataCleanStrict.NOT_NULL)) {
			return (0!=data.compareToIgnoreCase(NULL));
		}
		
		return false;
	}
	
	
	public abstract Enum<?> getCounterId();
	
	public String getColName() {
		return _colName;
	}
	
	public int getIndex() {
		return _colIdx;
	}
	
	public void setIndex(int index) {
		_colIdx = index;
	}

	public void getCounter(org.apache.hadoop.mapreduce.Mapper.Context context) {
		
		_counter = context.getCounter(getCounterId());
		
		if (null == _conf)
			_conf = context.getConfiguration();
		
	}

	public void getCounter(org.apache.hadoop.mapreduce.Reducer.Context context) {
		
		_counter = context.getCounter(getCounterId());
		
		if (null == _conf)
			_conf = context.getConfiguration();
		
	}
	
	public boolean inputVal(String strVal) {
		
		_colInput = strVal;
		
		if (isNull(_colInput)) {
			_colInput = NULL;
		}
		
		boolean bSucceed = isValidate(_colInput);
		
		if (bSucceed) {
			_colVal = _colInput;
		}
		else {
			_counter.increment(1);
		}
		
		return bSucceed;
	}

	public boolean inputVal(String[] strVals) throws IOException {
		
		int idx = getIndex();

		if (idx >= strVals.length || idx < 0) {
			dumpErr(" input vals length is " + strVals.length);
		}
		
		return inputVal(strVals[idx]);
	}


	public String toString() {		
		return _colVal;
	}
	
	public void clear() {
		_colVal = "";
	}
	

	public String Dump()
	{
		return this.getClass().getSimpleName() 
				+ " , index=" + getIndex() 
				+ ", COL: " + getColName() 
				+ ", input Value: " + _colInput
				+ ".\n";
	}
	

	public static String[] splitString(String line, int exp_len) {

		String[] items = new String[exp_len];
		int start_idx = 0;
		int end_idx = 0;
		int item_idx = 0;
		String item = "";
		while (-1 != (end_idx = line.indexOf(SEPRATOR, start_idx))) {
			item = line.substring(start_idx, end_idx);
			if (item.isEmpty())
				item = NULL;
			items[item_idx++] = item;
			start_idx = end_idx + 1;
			if (item_idx >= exp_len)
				break;
		}

		if (item_idx != (exp_len - 1)) {
			return null;
		}

		if (start_idx < line.length()) {
			items[item_idx++] = line.substring(start_idx, line.length());
		} else {
			items[item_idx++] = NULL;
		}

		return items;
	}
	
	
	protected void checkRequire (String colDef) {
		// The frist item is column name
		String [] defItems = colDef.trim().split(SEPRATOR_SPACE_SPLIT);
		
		_colName = defItems[0];
		
		if (defItems.length > 1) {
			
			if (isNotNull(colDef)) {
				_defValType = eDefaultType.IS_NOT_NULL;
			}
			else if (isDefZero(colDef)) {
				_defValType	= eDefaultType.IS_DEF_ZERO;			
			}
			else if (isNULLEmpty(colDef)) {
				_defValType	= eDefaultType.NULL_AS_EMPTY;
			}
		}
	}
	
	public void setAsKey () {
		if (_cleanData.equals(eDataCleanStrict.NO_STRICT)) {
			_cleanData = eDataCleanStrict.CUSTOME;
		}
	}
	

	protected static boolean isNotNull(String strVal) {
		Pattern pattern = Pattern.compile("([\\s\\S]*)((?i)NOT)([\\s]*)((?i)NULL)([\\s\\S]*)");
		Matcher mIsNull = pattern.matcher(strVal);
		boolean bIsNotNull = mIsNull.matches();

		return bIsNotNull;
	}

	protected static boolean isDefZero(String strVal) {
		Pattern pattern = Pattern.compile("([\\s\\S]*)((?i)DEF)([\\s]*)((?i)ZERO)([\\s\\S]*)");
		Matcher mIsNull = pattern.matcher(strVal);
		boolean bisNotZero = mIsNull.matches();

		return bisNotZero;
	}
	

	protected static boolean isNULLEmpty(String strVal) {
		Pattern pattern = Pattern.compile("([\\s\\S]*)((?i)NULL)([\\s]*)((?i)EMPTY)([\\s\\S]*)");
		Matcher mIsNull = pattern.matcher(strVal);
		boolean bisNotZero = mIsNull.matches();

		return bisNotZero;
	}

	protected static boolean isLike(String colDef) {
		Pattern pattern = Pattern.compile("([\\s\\S]*)((?i)LIKE)([\\s]*)");
		Matcher mIsNull = pattern.matcher(colDef);
		boolean bisNotZero = mIsNull.matches();

		return bisNotZero;
	}
	
	protected boolean isNull(String strVal) {
		
		boolean bIsNull = false;
		
		if (strVal.isEmpty()) {
			bIsNull = true;
		}
		else {
			Pattern pattern = Pattern.compile("((null)*|(NULL)*|-*|\\s*)*");
			Matcher mIsNull = pattern.matcher(strVal);
			bIsNull = mIsNull.matches();
		}

		return bIsNull;
	}
	
	protected void dumpErr(String errMsg) throws IOException {
		dumpErr(errMsg, true);
	}
	
	protected void dumpErr(String errMsg, boolean bThrowException) throws IOException {
		String dumpInfo = Dump() + errMsg;
		_LOG.error(dumpInfo);
		
		if (bThrowException) {
			throw new IOException(dumpInfo);
		}
	}

} // public static class ColParser