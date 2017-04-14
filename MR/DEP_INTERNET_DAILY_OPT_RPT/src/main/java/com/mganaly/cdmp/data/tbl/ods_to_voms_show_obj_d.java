package com.mganaly.cdmp.data.tbl;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mganaly.cdmp.data.type.ColParser;
import com.mganaly.cdmp.data.type.DataParserCfg;
import com.mganaly.cdmp.data.type.PageIdParser;
import com.mganaly.cdmp.data.type.StatisDayParser;
import com.mganaly.cdmp.data.type.eInvalidCounters;

/**
 * 
 * @author xxx
 * 
 * class cdmp_ods_to_voms_show_obj_d 
 *
 */

public class ods_to_voms_show_obj_d extends cdmp_base_tbl {

	private static final Log _LOG = LogFactory.getLog(ods_to_voms_show_obj_d.class);
		
	/*
	 * enum e_to_voms_show_obj_d
	 * 
	 * STATIS_DAY	20151111
	 * SHOW_OBJ_ID	70000026
	 * SHOW_OBJ_NAME
	 * SUP_NODE_ID	70000029
	 */
	private static enum e_to_voms_show_obj_d {
		STATIS_DAY(0)
		, SHOW_OBJ_ID(1)
		, SHOW_OBJ_NAME(2)
		, SUP_NODE_ID(3)
		, LENGTH(4);

		private int iCode = 0;

		private e_to_voms_show_obj_d (int code) {
			this.iCode = code;
		}

		public int v() {
			return this.iCode;
		}
	} // static enum e_td_pub_visit_log_d

	@Override
	protected int getIdx(String colName) {		
		e_to_voms_show_obj_d[] eCols = e_to_voms_show_obj_d.values();
		for (e_to_voms_show_obj_d eCol : eCols) {
			
			if (0 == colName.compareToIgnoreCase(eCol.name())) {
				return eCol.v();
			}
		}
		
		return -1;
	} //public static int getIdx(String colName)
	

	/*
	 * (non-Javadoc)
	 * @see com.mganaly.cdmp.data.cdmp_dw#getParser(java.lang.String, java.lang.String)
	 */
	public ColParser getParser(String group, String colName) throws IOException {
		ColParser newParser = super.getParser(group, colName);

		if (0 == group.compareToIgnoreCase(DataParserCfg.PARSER_TYPE_COL)) {
			int idx = getIdx(colName);
			if (-1 == idx) {
				throw new IOException ("Error : unkown type group: " 
							+ group + ", column name " + colName);
			}
			
			if (idx == e_to_voms_show_obj_d.STATIS_DAY.v()) {
				newParser = new STATIS_DAY_Parser(colName);
			}
			else if (idx == e_to_voms_show_obj_d.SHOW_OBJ_ID.v()) {
				newParser = new SHOW_OBJ_ID_Parser(colName);
			}
			else if (idx == e_to_voms_show_obj_d.SHOW_OBJ_NAME.v()) {
				newParser = new SHOW_OBJ_NAME_Parser(colName);
				
			} else if (idx == e_to_voms_show_obj_d.SUP_NODE_ID.v()) {
				newParser = new SUP_NODE_ID_Parser(colName);				
			} else {
				if (null == newParser)
					throw new IOException("Unkown type group: " + group + ", column name " + colName);
			} // switch (idx)
		}
		else {
			if (null == newParser)
			throw new IOException ("Error : unkown type group: " 
					+ group + ", column name" + colName);
		}

		return newParser;

	} // public ColParser getParser()
	
	public int length () {
		return e_to_voms_show_obj_d.LENGTH.v();
	}

	/*
	 * Public class SHOW_OBJ_ID_Parser 
	 */
	
	protected static class STATIS_DAY_Parser extends StatisDayParser {

		public STATIS_DAY_Parser(String name) {
			super(name);
			// TODO Auto-generated constructor stub
		}

		public int getIndex() {
			return e_to_voms_show_obj_d.STATIS_DAY.v();
		}
		
	} 
	
	/*
	 * Public class SUP_NODE_ID_Parser 
	 */
	
	protected static class SUP_NODE_ID_Parser extends PageIdParser {

		public SUP_NODE_ID_Parser(String name) {
			super(name);
			// TODO Auto-generated constructor stub
		}

		public int getIndex() {
			return e_to_voms_show_obj_d.SUP_NODE_ID.v();
		}
		
	}
	
	
	
	/*
	 * class SHOW_OBJ_NAME_Parser
	 * 
	 */
	protected static class SHOW_OBJ_NAME_Parser extends ColParser {
		

		public SHOW_OBJ_NAME_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_to_voms_show_obj_d.SHOW_OBJ_NAME.v();
		}
		

		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_SHOW_OBJ_NAME;
		}

		@Override
		protected boolean isDataClean(String data) {
			return true;
		}
	} //class CON_ATTR_2_NAME_Parser
	
	/*
	 * Public class SHOW_OBJ_ID_Parser 
	 */
	
	protected static class SHOW_OBJ_ID_Parser extends PageIdParser {

		public SHOW_OBJ_ID_Parser(String name) {
			super(name);
			// TODO Auto-generated constructor stub
		}

		public int getIndex() {
			return e_to_voms_show_obj_d.SHOW_OBJ_ID.v();
		}
	} 
	
} //class cdmp_ods_to_voms_show_obj_d
