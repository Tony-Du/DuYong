package com.mganaly.cdmp.data.tbl;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.mganaly.cdmp.data.type.ColParser;
import com.mganaly.cdmp.data.type.ContentIdParser;
import com.mganaly.cdmp.data.type.DataParserCfg;
import com.mganaly.cdmp.data.type.eInvalidCounters;

/**
 * 
 * 
 * 
DRAMA_CONTENT_ID	2201175284
SIN_CONTENT_ID	2201139361
SIN_ID	21
 *
 */
public class td_icms_drama_sin_rel extends cdmp_base_tbl {
	
	/*
	 * enum e_td_pub_visit_log_d
	 */
	private static enum e_td_icms_drama_sin_rel {
		DRAMA_CONTENT_ID(0)
		, SIN_CONTENT_ID(1)
		, SIN_ID(2)
		, LENGTH(3);
		
		
		private int iCode = 0;

		private e_td_icms_drama_sin_rel(int code) {
			this.iCode = code;
		}

		public int v() {
			return this.iCode;
		}
	} //enum e_td_icms_drama_sin_rel
	
	@Override
	protected int getIdx(String colName) {

		e_td_icms_drama_sin_rel [] eCols = e_td_icms_drama_sin_rel.values();
		for (e_td_icms_drama_sin_rel eCol : eCols) {

			if (0 == colName.compareToIgnoreCase(eCol.name())) {
				return eCol.v();
			}
		}

		return -1;
	} // int getIdx(String colName)
	
	public int length() {
		return e_td_icms_drama_sin_rel.LENGTH.v();
	}
	
	
	public ColParser getParser(String group, String colName) throws IOException {
		
		ColParser newParser = null;

		if (0 == group.compareToIgnoreCase(DataParserCfg.PARSER_TYPE_COL)) {
			int idx = getIdx(colName);
			if (idx == e_td_icms_drama_sin_rel.DRAMA_CONTENT_ID.v()) {
				newParser = new DRAMA_CONTENT_ID_Parser(colName);
			} else if (idx == e_td_icms_drama_sin_rel.SIN_CONTENT_ID.v()) {
				newParser = new SIN_CONTENT_ID_Parser(colName);
			} else if (idx == e_td_icms_drama_sin_rel.SIN_ID.v()) {
				newParser = new SIN_ID_Parser(colName);
			}
		}
		

		if (null == newParser) {
			newParser = super.getParser(group, colName);
			if (null == newParser) {
				throw new IOException("Unkown type group: " + group + ", column name" + colName);
			}
		}

		return newParser;

	} // public ColParser getParser()

	/*
	 * class DRAMA_CONTENT_ID_Parser
	 */
	protected static class DRAMA_CONTENT_ID_Parser extends ContentIdParser {
		
		public DRAMA_CONTENT_ID_Parser(String name) {
			super(name);
		}
	
		public int getIndex() {
			return e_td_icms_drama_sin_rel.DRAMA_CONTENT_ID.v();
		}
	} //DRAMA_CONTENT_ID_Parser
	

	/*
	 * class DRAMA_CONTENT_ID_Parser
	 */
	protected static class SIN_CONTENT_ID_Parser extends ContentIdParser {
		
		public SIN_CONTENT_ID_Parser(String name) {
			super(name);
		}
	
		public int getIndex() {
			return e_td_icms_drama_sin_rel.SIN_CONTENT_ID.v();
		}
	} //DRAMA_CONTENT_ID_Parser
	

	/*
	 * class SIN_ID_Parser
	 * 
	 */
	protected static class SIN_ID_Parser extends ColParser {
		

		public SIN_ID_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_icms_drama_sin_rel.SIN_ID.v();
		}
		

		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_SIN_ID;
		}

		protected boolean isDataClean(String strVal) {
			
			Pattern pattern = Pattern.compile("^\\d{1,5}$");
			Matcher isValidate = pattern.matcher(strVal);

			return isValidate.matches();
		}
	} //class SIN_ID_Parser


}
