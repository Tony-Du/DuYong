package com.mganaly.cdmp.data.tbl;

import java.io.IOException;
import com.mganaly.cdmp.data.type.ColParser;
import com.mganaly.cdmp.data.type.DataParserCfg;
import com.mganaly.cdmp.data.type.SvrNumParser;

public class td_userid_usernum extends cdmp_base_tbl {

	/**
	 * static enum e_td_userid_usernum
	 * eg
	STATIS_DAY	20160511
	USER_NUM	13521741931
	USER_ID	49069636
	LAST_MODIFY_TIME	20131031142543
	USERNUM_TYPE	02
	OPEN_ID	8613521741931
	REMARK	
	*/
	private static enum e_td_userid_usernum {
		STATIS_DAY(0)
		, USER_NUM(1)
		, USER_ID(2)
		, LAST_MODIFY_TIME(3)
		, USERNUM_TYPE(4)
		, OPEN_ID(5), 
		LENGTH(7);

		private int iCode = 0;

		private e_td_userid_usernum(int code) {
			this.iCode = code;
		}

		public int v() {
			return this.iCode;
		}
	} // static enum e_td_userid_usernum
	
	/*
	 * enum e_counters
	public enum e_counters {
		INV_USER_NUM
		, INV_USER_ID
		, INV_LAST_MODIFY_TIME
		, INV_USERNUM_TYPE
		, INV_OPEN_ID
	}
	 */

	public td_userid_usernum() {
	}

	@Override
	public int length() {
		return e_td_userid_usernum.LENGTH.v();
	}
	
	@Override
	protected int getIdx(String colName) {
		
		e_td_userid_usernum[] eCols = e_td_userid_usernum.values();
		for (e_td_userid_usernum eCol : eCols) {
			
			if (0 == colName.compareToIgnoreCase(eCol.name())) {
				return eCol.v();
			}
		}
		
		return -1;

	} //public static int getIdx(String colName)
	

	public ColParser getParser(String group, String colName) throws IOException {
		
		ColParser newParser = super.getParser(group, colName);
		
		if (0 == group.compareToIgnoreCase(DataParserCfg.PARSER_TYPE_COL)) {
			int idx = getIdx(colName);			
			if (idx == e_td_userid_usernum.USER_NUM.v()) {
				newParser = new USER_NUM_Parser(colName);
			} else if (idx == e_td_userid_usernum.USER_ID.v()) {
				newParser = new USER_ID_Parser(colName);
			} else if (idx == e_td_userid_usernum.USERNUM_TYPE.v()) {
				newParser = new USERNUM_TYPE_Parser(colName);
			} else {
				if (null == newParser)
				throw new IOException ("Error : unkown type group: " 
							+ group + ", column name" + colName);
			} // switch (idx)
		}
		else {
			if (null == newParser)
				throw new IOException("Unkown type group: " + group + ", column name " + colName);
		}

		return newParser;

	} // public ColParser getParser()
	
	
	//  ---------------------------
	/// class USER_NUM_Parser
	//  ---------------------------
	private static class USER_NUM_Parser extends SvrNumParser {
	
		public USER_NUM_Parser(String name) {
			super(name);
			// TODO Auto-generated constructor stub
		}

		@Override
		public int getIndex() {
			return e_td_userid_usernum.USER_NUM.v();
		}

	}
	
	//  ---------------------------
	/// class USER_ID_Parser
	//  ---------------------------
	private static class USER_ID_Parser extends SvrNumParser {
	
		public USER_ID_Parser(String name) {
			super(name);
			// TODO Auto-generated constructor stub
		}

		@Override
		public int getIndex() {
			return e_td_userid_usernum.USER_ID.v();
		}

	}	

	//  ---------------------------
	/// class USERNUM_TYPE_Parser
	//  ---------------------------
	private static class USERNUM_TYPE_Parser extends SvrNumParser {
	
		public USERNUM_TYPE_Parser(String name) {
			super(name);
			// TODO Auto-generated constructor stub
		}

		@Override
		public int getIndex() {
			return e_td_userid_usernum.USERNUM_TYPE.v();
		}

	}

	

} // class cdmp_td_userid_usernum
