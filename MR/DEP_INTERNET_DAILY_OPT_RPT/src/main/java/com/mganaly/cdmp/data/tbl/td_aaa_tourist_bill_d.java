package com.mganaly.cdmp.data.tbl;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mganaly.cdmp.data.type.ChnIdNewParser;
import com.mganaly.cdmp.data.type.ColParser;
import com.mganaly.cdmp.data.type.ContentIdParser;
import com.mganaly.cdmp.data.type.DataParserCfg;
import com.mganaly.cdmp.data.type.PageIdParser;
import com.mganaly.cdmp.data.type.ProgramIdParser;
import com.mganaly.cdmp.data.type.StatisDayParser;
import com.mganaly.cdmp.data.type.SvrNumParser;
import com.mganaly.cdmp.data.type.TermProdIdParser;
import com.mganaly.cdmp.data.type.eInvalidCounters;
import com.mganaly.cdmp.data.type.FloatParser;

/*
 * 
 * class cdmp_dw_td_aaa_tourist_bill_d
 * 
 * 
 * 
 * 
 * 
 * 
 * V20160525
STATIS_DAY	20131024
SERV_NUMBER	0291013748633
BILL_TIME	20131024150127
CP_ID	699013
SERVICE_TYPE	16
CHARGE_TYPE	7
BILL_FEE	0
PRODUCT_ID	2028593168
CONTENT_ID	0
USE_DUR	160.00
USE_FLOW	673792.00
OPT_TYPE	0
CHN_ID_SOURCE	0101_10030001127
BROADCAST_TYPE	0
NET_TYPE	2
PROGRAM_ID	501013418
PROGRAM_FATHER_ID	10148947
CHN_ID	0101
TERM_PROD_ID	P0000009
BUSINESS_ID	B1000100020
SUB_BUSI_ID	S1000100039
VIRT_BUSI_ID	9999
NEED_FEE	
FREE_TYPE	
EXTERNAL_PRODUCT	
SALES_ID	
VERSION_ID	
CHN_99000	
CHN_ID_NEW	10030001127
NEW_PRODUCT_ID	
BROADCAST_TYPE_NEW
SESSION_ID	
PAGE_ID	
POSE_ID	
IMEI	
BROADCAST_IP	
PROV_ID	
ENCRYPTKEY	
TERMINAL_IP	(ignore)
 */
public class td_aaa_tourist_bill_d extends cdmp_base_tbl {
	
	private static final Log _LOG = LogFactory.getLog(td_aaa_tourist_bill_d.class);

	/*
	 * enum e_td_aaa_tourist_bill_d
	 * 
	 * 
	 * 1.3.2	表结构差异
		序号	字段名	FI	CDMP
		1	STATIS_DAY	√	√
		..….	..….	..….	..….
		31	INSERT_DAY	╳	√
		..….	..….	..….	..….
		34	LOG_SOURCE_NEW	╳	√
		..….	..….	..….	..….

	 * 
	 */
	private static enum e_td_aaa_tourist_bill_d {
		STATIS_DAY(0)
		, SERV_NUMBER(1)
		, CONTENT_ID(8)
		, USE_DUR (9)
		, USE_FLOW(10)
		, TERM_TYPE(13)
		, PROGRAM_ID(15)
		, TERM_PROD_ID(18)
		, CHN_ID_NEW(28)
		, PAGE_ID (33) 
		, POSE_ID (34)
		, LENGTH(40);

		private int iCode = 0;

		private e_td_aaa_tourist_bill_d(int code) {
			this.iCode = code;
		}

		public final int v() {
			return this.iCode;
		}
	} // static enum e_td_aaa_bill_d

	@Override
	protected int getIdx(String colName) {
		
		e_td_aaa_tourist_bill_d[] eCols = e_td_aaa_tourist_bill_d.values();
		for (e_td_aaa_tourist_bill_d eCol : eCols) {
			
			if (0 == colName.compareToIgnoreCase(eCol.name())) {
				return eCol.v();
			}
		}
		
		return -1;
		
	} // getIdx

	public ColParser getParser(String group, String colName) throws IOException {
		
		ColParser newParser = null;

		if (0 == group.compareToIgnoreCase(DataParserCfg.PARSER_TYPE_COL)) {
			
			int idx = getIdx(colName);
			
			if (idx == e_td_aaa_tourist_bill_d.STATIS_DAY.v()) {
				newParser = new STATIS_DAY_Parser(colName);
			} else if (idx == e_td_aaa_tourist_bill_d.SERV_NUMBER.v()) {
				newParser = new SERV_NUMBER_Parser(colName);
			} else if (idx == e_td_aaa_tourist_bill_d.USE_DUR.v()) {
				newParser = new USE_DUR_Parser(colName);
			} else if (idx == e_td_aaa_tourist_bill_d.USE_FLOW.v()) {
				newParser = new USE_FLOW_Parser(colName);
			} else if (idx == e_td_aaa_tourist_bill_d.CONTENT_ID.v()) {
				newParser = new CONTENT_ID_Parser(colName);
			} else if (idx == e_td_aaa_tourist_bill_d.PROGRAM_ID.v()) {
				newParser = new PROGRAM_ID_Parser(colName);
			} else if (idx == e_td_aaa_tourist_bill_d.TERM_PROD_ID.v()) {
				newParser = new TERM_PROD_ID_Parser(colName);				
			} else if (idx == e_td_aaa_tourist_bill_d.CHN_ID_NEW.v()) {
				newParser = new CHN_ID_NEW_Parser(colName);
			} else if (idx == e_td_aaa_tourist_bill_d.PAGE_ID.v()) {
				newParser = new PAGE_ID_Parser(colName);
			} else if (idx == e_td_aaa_tourist_bill_d.POSE_ID.v()) {
				newParser = new POSE_ID_Parser(colName);
			}
		}
		
		if (null == newParser) {
			newParser = super.getParser(group, colName);			
			checkParser(newParser, group, colName);
		}
		
		return newParser;

	} // public static ColParser getParser
	

	public int length () {
		return e_td_aaa_tourist_bill_d.LENGTH.v();
	}
	

	/*
	 * Public class STATIS_DAY_Parser
	 */

	protected static class STATIS_DAY_Parser extends StatisDayParser {

		public STATIS_DAY_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_aaa_tourist_bill_d.STATIS_DAY.v();
		}
	}

	/*
	 * SERV_NUMBER_Parser
	 * 
	 */
	protected static class SERV_NUMBER_Parser extends SvrNumParser {
		

		public SERV_NUMBER_Parser(String name) {
			super(name);
		}

		@Override
		public int getIndex() {
			return e_td_aaa_tourist_bill_d.SERV_NUMBER.v();
		}

		@Override
		protected boolean isDataClean(String strVal) {
			Pattern pattern = Pattern.compile("^[A-Za-z0-9]{1,100}$");
			Matcher isValidate = pattern.matcher(strVal);
			return isValidate.matches();
		}
	}

	/*
	 * CONTENT_ID_Parser
	 */
	protected static class CONTENT_ID_Parser extends ContentIdParser {

		public CONTENT_ID_Parser(String name) {
			super(name);
		}

		@Override
		public int getIndex() {
			return e_td_aaa_tourist_bill_d.CONTENT_ID.v();
		}
	}
	
	/*
	 *	USE_DUR_Parser 
	 * 
	 */
	protected static class USE_DUR_Parser extends FloatParser {
		public USE_DUR_Parser(String name) {
			super(name);
		}

		@Override
		public int getIndex() {
			return e_td_aaa_tourist_bill_d.USE_DUR.v();
		}

		@Override
		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_USE_DUR;
		}
	} 
	
	/*
	 *	USE_FLOW_Parser 
	 * 
	 */
	protected static class USE_FLOW_Parser extends FloatParser {
		public USE_FLOW_Parser(String name) {
			super(name);
		}

		@Override
		public int getIndex() {
			return e_td_aaa_tourist_bill_d.USE_FLOW.v();
		}

		@Override
		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_USE_FLOW;
		}

	} 

	/*
	 * PROGRAM_ID_Parser
	 * 
	 */
	protected static class PROGRAM_ID_Parser extends ProgramIdParser {
		public PROGRAM_ID_Parser(String name) {
			super(name);
		}

		@Override
		public int getIndex() {
			return e_td_aaa_tourist_bill_d.PROGRAM_ID.v();
		}
	}
	

	/*
	 * Public class STATIS_DAY_Parser
	 */

	protected static class TERM_PROD_ID_Parser extends TermProdIdParser {
		public TERM_PROD_ID_Parser(String name) {
			super(name);
		}

		@Override
		public int getIndex() {
			return e_td_aaa_tourist_bill_d.TERM_PROD_ID.v();
		}

	}


	/*
	 * CHN_ID_NEW_Parser
	 * 
	 */

	protected static class CHN_ID_NEW_Parser extends ChnIdNewParser {
		public CHN_ID_NEW_Parser(String name) {
			super(name);
		}

		@Override
		public int getIndex() {
			return e_td_aaa_tourist_bill_d.CHN_ID_NEW.v();
		}
	}
	/*
	 * Public class PAGE_ID_Parser 
	 */
	
	protected static class PAGE_ID_Parser extends PageIdParser {
		public PAGE_ID_Parser(String name) {
			super(name);
		}

		@Override
		public int getIndex() {
			return e_td_aaa_tourist_bill_d.PAGE_ID.v();
		}
		
	} 
	

	/*
	 * Public class POSE_ID_Parser 
	 */
	
	protected static class POSE_ID_Parser extends PageIdParser {
		public POSE_ID_Parser(String name) {
			super(name);
		}
		

		@Override
		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_POSE_ID;
		}

		@Override
		public int getIndex() {
			return e_td_aaa_tourist_bill_d.POSE_ID.v();
		}
		
	}

} // public class cdmp_dw_td_aaa_bill_d