package com.mganaly.cdmp.data.tbl;

import java.io.IOException;

import com.mganaly.cdmp.data.type.ColParser;
import com.mganaly.cdmp.data.type.ContentClassParser;
import com.mganaly.cdmp.data.type.ContentIdParser;
import com.mganaly.cdmp.data.type.DataParserCfg;
import com.mganaly.cdmp.data.type.eInvalidCounters;

/**
 * 
 * class td_cms_content_class_d
 * 
 * 
 * 
 * 
CONTENT_ID	2047011718
CON_CLASS_1_NAME	音频
CON_CLASS_2_NAME	内容形态
CON_TAG_1_NAME	连载
CON_TAG_2_NAME	
CON_TAG_3_NAME	
CON_TAG_4_NAME	
CON_TAG_5_NAME
 *
 */
public class td_cms_content_class_d extends cdmp_base_tbl {
	
	/*
	 * enum e_td_pub_visit_log_d
	 */
	private static enum e_td_cms_content_class_d {
		CONTENT_ID(0)
		, CON_CLASS_1_NAME(1)
		, CON_CLASS_2_NAME(2)
		, CON_TAG_1_NAME(3)
		, CON_TAG_2_NAME(4)
		, CON_TAG_3_NAME(5)
		, CON_TAG_4_NAME(6)
		, CON_TAG_5_NAME(7)
		, LENGTH(8);

		private int iCode = 0;

		private e_td_cms_content_class_d(int code) {
			this.iCode = code;
		}

		public int v() {
			return this.iCode;
		}
	} // enum e_td_oms_program_d
	
	@Override
	protected int getIdx(String colNameDef) {
		String colName = colNameDef.trim().split(ColParser.SEPRATOR_SPACE_SPLIT)[0];

		e_td_cms_content_class_d [] eCols = e_td_cms_content_class_d.values();
		for (e_td_cms_content_class_d eCol : eCols) {

			if (0 == colName.compareToIgnoreCase(eCol.name())) {
				return eCol.v();
			}
		}

		return -1;
	} // public static int getIdx(String colName)
	
	public int length() {
		return e_td_cms_content_class_d.LENGTH.v();
	}

	public ColParser getParser(String group, String colName) throws IOException {
		
		ColParser newParser = null;
		
		if (0 == group.compareToIgnoreCase(DataParserCfg.PARSER_TYPE_COL)) {
			int idx = getIdx(colName);
			if (idx == e_td_cms_content_class_d.CONTENT_ID.v()) {
				newParser = new CONTENT_ID_Parser(colName);
			} else if (idx == e_td_cms_content_class_d.CON_CLASS_1_NAME.v()) {
				newParser = new CON_CLASS_1_NAME_Parser(colName);
			} else if (idx == e_td_cms_content_class_d.CON_CLASS_2_NAME.v()) {
				newParser = new CON_CLASS_2_NAME_Parser(colName);
			} else if (idx == e_td_cms_content_class_d.CON_TAG_1_NAME.v()) {
				newParser = new CON_TAG_1_NAME_Parser(colName);
			} else if (idx == e_td_cms_content_class_d.CON_TAG_2_NAME.v()) {
				newParser = new CON_TAG_2_NAME_Parser(colName);
			} else if (idx == e_td_cms_content_class_d.CON_TAG_3_NAME.v()) {
				newParser = new CON_TAG_3_NAME_Parser(colName);
			} else if (idx == e_td_cms_content_class_d.CON_TAG_4_NAME.v()) {
				newParser = new CON_TAG_4_NAME_Parser(colName);
			} else if (idx == e_td_cms_content_class_d.CON_TAG_5_NAME.v()) {
				newParser = new CON_TAG_5_NAME_Parser(colName);
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
			return e_td_cms_content_class_d.CONTENT_ID.v();
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
			return e_td_cms_content_class_d.CON_CLASS_1_NAME.v();
		}
		

		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_CON_CLASS_1_NAME;
		}
	} //class CON_CLASS_1_NAME_Parser

	/*
	 * class CON_CLASS_2_NAME_Parser
	 * 
	 */
	protected static class CON_CLASS_2_NAME_Parser extends ContentClassParser {
		

		public CON_CLASS_2_NAME_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_cms_content_class_d.CON_CLASS_2_NAME.v();
		}
		

		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_CON_CLASS_2_NAME;
		}
	} //class CON_CLASS_2_NAME_Parser
	
	/*
	 * class CON_TAG_1_NAME_Parser
	 * 
	 */
	protected static class CON_TAG_1_NAME_Parser extends ColParser {
		

		public CON_TAG_1_NAME_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_cms_content_class_d.CON_TAG_1_NAME.v();
		}
		

		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_CON_TAG_1_NAME;
		}

		@Override
		protected boolean isDataClean(String data) {
			// TODO Auto-generated method stub
			return true;
		}
	} //class CON_TAG_1_NAME_Parser

	/*
	 * class CON_TAG_2_NAME_Parser
	 * 
	 */
	protected static class CON_TAG_2_NAME_Parser extends ColParser {
		

		public CON_TAG_2_NAME_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_cms_content_class_d.CON_TAG_2_NAME.v();
		}
		

		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_CON_TAG_2_NAME;
		}

		@Override
		protected boolean isDataClean(String data) {
			// TODO Auto-generated method stub
			return true;
		}
	} //class CON_TAG_2_NAME_Parser
	
	/*
	 * class CON_TAG_3_NAME_Parser
	 * 
	 */
	protected static class CON_TAG_3_NAME_Parser extends ColParser {
		

		public CON_TAG_3_NAME_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_cms_content_class_d.CON_TAG_3_NAME.v();
		}
		

		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_CON_TAG_3_NAME;
		}

		@Override
		protected boolean isDataClean(String data) {
			// TODO Auto-generated method stub
			return true;
		}
	} //class CON_TAG_3_NAME_Parser
	
	
	/*
	 * class CON_TAG_4_NAME_Parser
	 * 
	 */
	protected static class CON_TAG_4_NAME_Parser extends ColParser {
		

		public CON_TAG_4_NAME_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_cms_content_class_d.CON_TAG_4_NAME.v();
		}
		

		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_CON_TAG_4_NAME;
		}

		@Override
		protected boolean isDataClean(String data) {
			// TODO Auto-generated method stub
			return true;
		}
	} //class CON_TAG_4_NAME_Parser
	
	
	/*
	 * class CON_TAG_5_NAME_Parser
	 * 
	 */
	protected static class CON_TAG_5_NAME_Parser extends ColParser {
		

		public CON_TAG_5_NAME_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_cms_content_class_d.CON_TAG_5_NAME.v();
		}
		

		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_CON_TAG_5_NAME;
		}

		@Override
		protected boolean isDataClean(String data) {
			// TODO Auto-generated method stub
			return true;
		}
	} //class CON_TAG_5_NAME_Parser

}
