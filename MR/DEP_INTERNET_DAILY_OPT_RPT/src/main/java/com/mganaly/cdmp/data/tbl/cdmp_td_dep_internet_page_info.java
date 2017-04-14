package com.mganaly.cdmp.data.tbl;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mganaly.cdmp.data.type.ColParser;
import com.mganaly.cdmp.data.type.DataParserCfg;
import com.mganaly.cdmp.data.type.PageIdParser;


/**
 * 
 * @author XXX
 * 
 * class cdmp_td_dep_internet_page_info
 * 
 * 
0 COL:PAGE_ID
1 COL:PAGE_NAME
 *
 */
public class cdmp_td_dep_internet_page_info extends cdmp_base_tbl {
	
	private static final Log _LOG = LogFactory.getLog(cdmp_td_dep_internet_page_info.class);
	
	private  static final String static_path = "public/cdmp/"
			+ "td_dep_internet_page_info/td_dep_internet_page_info.dat";


	/*
	 * enum e_td_dep_internet_page_info
	 */
	private static enum e_td_dep_internet_page_info {
		PAGE_ID(0)
		, PAGE_NAME(1)
		, LENGTH(2);

		private int iCode = 0;

		private e_td_dep_internet_page_info(int code) {
			this.iCode = code;
		}

		public int v() {
			return this.iCode;
		}
	} // static enum e_td_pub_visit_log_d
		

	public cdmp_td_dep_internet_page_info() {
	}

	@Override
	protected int getIdx(String colName) {
		
		e_td_dep_internet_page_info[] eCols = e_td_dep_internet_page_info.values();
		for (e_td_dep_internet_page_info eCol : eCols) {
			
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
			if (-1 == idx) {
				throw new IOException ("Error : unkown type group: " 
							+ group + ", column name " + colName);
			}
			
			if (idx == e_td_dep_internet_page_info.PAGE_ID.v()) {
				newParser = new PAGE_ID_Parser(colName);
			} else {
				if (null == newParser)
				throw new IOException ("Error : unkown type group: " 
							+ group + ", column name" + colName);
			} // switch (idx)
		}
		else {
			if (null == newParser)
				throw new IOException("Error : unkown type group: " + group + ", column name" + colName);
		}

		return newParser;

	} // public ColParser getParser()
	
	public int length () {
		return e_td_dep_internet_page_info.LENGTH.v();
	}
	
	
	
	public static String getPath () {
		return  static_path;
	}
	
	/*
	 * Public class PAGE_ID_Parser 
	 */
	
	public static class PAGE_ID_Parser extends PageIdParser {
		public PAGE_ID_Parser(String name) {
			super(name);
			// TODO Auto-generated constructor stub
		}

		@Override
		public int getIndex() {
			return e_td_dep_internet_page_info.PAGE_ID.v();
		}

	} 

}
