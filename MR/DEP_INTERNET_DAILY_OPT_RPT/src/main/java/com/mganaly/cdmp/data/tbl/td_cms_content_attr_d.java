package com.mganaly.cdmp.data.tbl;

import java.io.IOException;
import com.mganaly.cdmp.data.type.ColParser;
import com.mganaly.cdmp.data.type.ContentClassParser;
import com.mganaly.cdmp.data.type.ContentIdParser;
import com.mganaly.cdmp.data.type.DataParserCfg;
import com.mganaly.cdmp.data.type.eInvalidCounters;

/**
 * 
 * class td_cms_content_attr_d
 * 
 * 
 * 
 * 
CONTENT_ID	2055254386
CON_CLASS_1_NAME	电视剧
CON_ATTR_2_NAME	所属片名
CON_ATTR_2_VALUE	小两口的爆笑生活
 *
 */
public class td_cms_content_attr_d extends cdmp_base_tbl {
	
	/*
	 * enum e_td_pub_visit_log_d
	 */
	private static enum e_td_cms_content_attr_d {
		CONTENT_ID(0)
		, CON_CLASS_1_NAME(1)
		, CON_ATTR_2_NAME(2)
		, CON_ATTR_2_VALUE(3)
		, LENGTH(4);

		private int iCode = 0;

		private e_td_cms_content_attr_d(int code) {
			this.iCode = code;
		}

		public int v() {
			return this.iCode;
		}
	} // enum e_td_oms_program_d
	
	@Override
	protected int getIdx(String colNameDef) {

		String colName = colNameDef.trim().split(ColParser.SEPRATOR_SPACE_SPLIT)[0];
		
		e_td_cms_content_attr_d [] eCols = e_td_cms_content_attr_d.values();
		for (e_td_cms_content_attr_d eCol : eCols) {

			if (0 == colName.compareToIgnoreCase(eCol.name())) {
				return eCol.v();
			}
		}

		return -1;
	} // public static int getIdx(String colName)
	
	public int length() {
		return e_td_cms_content_attr_d.LENGTH.v();
	}

	public ColParser getParser(String group, String colName) throws IOException {
		
		ColParser newParser = null;
		
		if (0 == group.compareToIgnoreCase(DataParserCfg.PARSER_TYPE_COL)) {
			int idx = getIdx(colName);
			if (idx == e_td_cms_content_attr_d.CONTENT_ID.v()) {
				newParser = new CONTENT_ID_Parser(colName);
			} else if (idx == e_td_cms_content_attr_d.CON_CLASS_1_NAME.v()) {
				newParser = new CON_CLASS_1_NAME_Parser(colName);
			} else if (idx == e_td_cms_content_attr_d.CON_ATTR_2_NAME.v()) {
				newParser = new CON_ATTR_2_NAME_Parser(colName);
			} else if (idx == e_td_cms_content_attr_d.CON_ATTR_2_VALUE.v()) {
				newParser = new CON_ATTR_2_VALUE_Parser(colName);
			}
		}		
		
		if (null == newParser) {
			newParser = super.getParser(group, colName);			
			checkParser(newParser, group, colName);
		}
		
		return newParser;

	} // public ColParser getParser()
	
	
	/*
	 * class CONTENT_ID_Parser
	 */
	protected static class CONTENT_ID_Parser extends ContentIdParser {
		
		public CONTENT_ID_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_cms_content_attr_d.CONTENT_ID.v();
		}
	}
	

	/*
	 * class CON_CLASS_1_NAME_Parser
	 * 
	 */
	protected static class CON_CLASS_1_NAME_Parser extends ContentClassParser {
		

		public CON_CLASS_1_NAME_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_cms_content_attr_d.CON_CLASS_1_NAME.v();
		}
		

		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_CON_CLASS_1_NAME;
		}
	} //class CON_CLASS_1_NAME_Parser
	
	/*
	 * class CON_ATTR_2_NAME_Parser
	 * 
	 */
	protected static class CON_ATTR_2_NAME_Parser extends ColParser {
		

		public CON_ATTR_2_NAME_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_cms_content_attr_d.CON_ATTR_2_NAME.v();
		}
		

		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_CON_ATTR_2_NAME;
		}

		@Override
		protected boolean isDataClean(String data) {
			// TODO Auto-generated method stub
			return true;
		}
	} //class CON_ATTR_2_NAME_Parser
	
	

	/*
	 * class CON_ATTR_2_VALUE_Parser
	 * 
	 */
	protected static class CON_ATTR_2_VALUE_Parser extends ColParser {
		

		public CON_ATTR_2_VALUE_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_cms_content_attr_d.CON_ATTR_2_VALUE.v();
		}
		

		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_CON_ATTR_2_VALUE;
		}

		@Override
		protected boolean isDataClean(String data) {
			// TODO Auto-generated method stub
			return true;
		}
	} //class CON_ATTR_2_VALUE_Parser
	
	

} //class td_cms_content_attr_d
