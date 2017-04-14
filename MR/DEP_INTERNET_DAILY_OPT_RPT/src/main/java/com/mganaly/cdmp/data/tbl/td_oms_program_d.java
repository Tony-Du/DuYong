package com.mganaly.cdmp.data.tbl;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.mganaly.cdmp.data.type.CP_ID_Parser;
import com.mganaly.cdmp.data.type.ColParser;
import com.mganaly.cdmp.data.type.ContentIdParser;
import com.mganaly.cdmp.data.type.DataParserCfg;
import com.mganaly.cdmp.data.type.DecimalParser;
import com.mganaly.cdmp.data.type.ProductIdParser;
import com.mganaly.cdmp.data.type.ProgramIdParser;
import com.mganaly.cdmp.data.type.eInvalidCounters;

/**
 * class dw_td_oms_program_d
 * 
PROGRAM_ID	600319784
CP_ID	699019
CONTENT_STATUS	
PROGRAM_STATUS	1
CREATE_TIME	20141610032758
MODIFY_TIME	20141610032758
MODIFY_LOGIN	
CREATE_LOGIN	
PROGRAM_NAME	¡¶¼ÒÍ¥½ÌÊ¦¡·136ÕæÏà´ó°×
CONTENT_ID	3001331230
PROGRAM_TYPE	1
PRODUCT_ID	
IS_FIRST
IS_MOBILE_INTO	
NEW_PRODUCT_ID	1001381
STUDIO_CONTROL_ID	699004
 * 
 * 
 */
public class td_oms_program_d extends cdmp_base_tbl {
	
	/*
	 * enum e_td_pub_visit_log_d
	 */
	private static enum e_td_oms_program_d {
		PROGRAM_ID(0)
		, CP_ID(1) // 6 d
		, CONTENT_STATUS(2)
		, PROGRAM_NAME(8)
		, CONTENT_ID(9)
		, PROGRAM_TYPE(10)
		, PRODUCT_ID(11)
		, NEW_PRODUCT_ID(14)
		, STUDIO_CONTROL_ID(15)
		, LENGTH(16);

		private int iCode = 0;

		private e_td_oms_program_d(int code) {
			this.iCode = code;
		}

		public int v() {
			return this.iCode;
		}
	} // enum e_td_oms_program_d
	
	@Override
	protected int getIdx(String colName) {

		e_td_oms_program_d [] eCols = e_td_oms_program_d.values();
		for (e_td_oms_program_d eCol : eCols) {

			if (0 == colName.compareToIgnoreCase(eCol.name())) {
				return eCol.v();
			}
		}

		return -1;
	} // public static int getIdx(String colName)
	
	@Override
	public int length() {
		return e_td_oms_program_d.LENGTH.v();
	}

	public ColParser getParser(String group, String colName) throws IOException {
		
		ColParser newParser = null;

		if (0 == group.compareToIgnoreCase(DataParserCfg.PARSER_TYPE_COL)) {
			int idx = getIdx(colName);
			if (idx == e_td_oms_program_d.PROGRAM_ID.v()) {
				newParser = new PROGRAM_ID_Parser(colName);
			} else if (idx == e_td_oms_program_d.CP_ID.v()) {
				newParser = new td_CP_ID_Parser(colName);
			} else if (idx == e_td_oms_program_d.CONTENT_STATUS.v()) {
				newParser = new CONTENT_STATUS_Parser(colName);
			} else if (idx == e_td_oms_program_d.PROGRAM_NAME.v()) {
				newParser = new PROGRAM_NAME_Parser(colName);
			} else if (idx == e_td_oms_program_d.CONTENT_ID.v()) {
				newParser = new CONTENT_ID_Parser(colName);
			} else if (idx == e_td_oms_program_d.PROGRAM_TYPE.v()) {
				newParser = new PROGRAM_TYPE_Parser(colName);
			} else if (idx == e_td_oms_program_d.PRODUCT_ID.v()) {
				newParser = new PRODUCT_ID_Parser(colName);
			} else if (idx == e_td_oms_program_d.NEW_PRODUCT_ID.v()) {
				newParser = new NEW_PRODUCT_ID_Parser(colName);
			} else if (idx == e_td_oms_program_d.STUDIO_CONTROL_ID.v()) {
				newParser = new STUDIO_CONTROL_ID_Parser(colName);
			} 
		}		
		
		if (null == newParser) {
			newParser = super.getParser(group, colName);			
			checkParser(newParser, group, colName);
		}
		
		return newParser;

	} // public ColParser getParser()
	
	

	/*
	 * class PROGRAM_ID_Parser
	 * 
	 */
	protected static class PROGRAM_ID_Parser extends ProgramIdParser {
		

		public PROGRAM_ID_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_oms_program_d.PROGRAM_ID.v();
		}
	}
	

	/*
	 * class CP_ID_Parser
	 * 
	 */
	protected static class td_CP_ID_Parser extends CP_ID_Parser {
		

		public td_CP_ID_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_oms_program_d.CP_ID.v();
		}
		
	} //class CP_ID_Parser
	

	/*
	 * class CONTENT_STATUS_Parser
	 * 
	 */
	protected static class CONTENT_STATUS_Parser extends DecimalParser {
		

		public CONTENT_STATUS_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_oms_program_d.CONTENT_STATUS.v();
		}
		

		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_CONTENT_STATUS;
		}

		protected boolean isDataClean(String strVal) {
			
			return isInRange(strVal, 1, 2);
		}
	} //class CONTENT_STATUS_Parser
	

	/*
	 * class PROGRAM_NAME_Parser
	 * 
	 */
	protected static class PROGRAM_NAME_Parser extends ColParser {
		

		public PROGRAM_NAME_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_oms_program_d.PROGRAM_NAME.v();
		}
		

		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_PROGRAM_NAME;
		}

		@Override
		protected boolean isDataClean(String data) {
			// TODO Auto-generated method stub
			return true;
		}
	} //class PROGRAM_NAME_Parser
	

	/*
	 * class CONTENT_ID_Parser
	 */
	protected static class CONTENT_ID_Parser extends ContentIdParser {
		
		public CONTENT_ID_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_oms_program_d.CONTENT_ID.v();
		}
	}
	
	/*
	 * class PROGRAM_TYPE_Parser
	 * 
	 */
	protected static class PROGRAM_TYPE_Parser extends DecimalParser {
		

		public PROGRAM_TYPE_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_oms_program_d.PROGRAM_TYPE.v();
		}
		

		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_PROGRAM_TYPE;
		}

		protected boolean isDataClean(String strVal) {
			return isInRange(strVal, 1, 2);
		}
	} //class PROGRAM_TYPE_Parser
	

	/*
	 * class PRODUCT_ID_Parser
	 * 
	 */
	protected static class PRODUCT_ID_Parser extends ProductIdParser {
		

		public PRODUCT_ID_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_oms_program_d.PRODUCT_ID.v();
		}

	} //class PRODUCT_ID_Parser
	

	/*
	 * class NEW_PRODUCT_ID_Parser
	 * 
	 */
	protected static class NEW_PRODUCT_ID_Parser extends PRODUCT_ID_Parser {
		

		public NEW_PRODUCT_ID_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_oms_program_d.NEW_PRODUCT_ID.v();
		}
		

		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_NEW_PRODUCT_ID;
		}

		protected boolean isDataClean(String strVal) {
			
			Pattern pattern = Pattern.compile("^\\d{5,10}$");
			Matcher isValidate = pattern.matcher(strVal);

			return isValidate.matches();
		}
	} //class NEW_PRODUCT_ID_Parser
	
	
	/*
	 * class STUDIO_CONTROL_ID_Parser
	 * 
	 */
	protected static class STUDIO_CONTROL_ID_Parser extends ColParser {
		

		public STUDIO_CONTROL_ID_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_oms_program_d.STUDIO_CONTROL_ID.v();
		}
		

		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_STUDIO_CONTROL_ID;
		}

		protected boolean isDataClean(String strVal) {

			Pattern pattern = Pattern.compile("^\\d{5,10}$");
			Matcher isValidate = pattern.matcher(strVal);

			return isValidate.matches();
		}
	} //class STUDIO_CONTROL_ID_Parser

}
