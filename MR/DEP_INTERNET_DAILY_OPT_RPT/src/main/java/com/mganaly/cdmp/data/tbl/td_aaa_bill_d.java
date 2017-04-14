package com.mganaly.cdmp.data.tbl;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mganaly.cdmp.data.type.CP_ID_Parser;
import com.mganaly.cdmp.data.type.ChnIdNewParser;
import com.mganaly.cdmp.data.type.ColParser;
import com.mganaly.cdmp.data.type.ContentIdParser;
import com.mganaly.cdmp.data.type.DataParserCfg;
import com.mganaly.cdmp.data.type.DecimalParser;
import com.mganaly.cdmp.data.type.PageIdParser;
import com.mganaly.cdmp.data.type.ProductIdParser;
import com.mganaly.cdmp.data.type.ProgramIdParser;
import com.mganaly.cdmp.data.type.StatisDayParser;
import com.mganaly.cdmp.data.type.SvrNumParser;
import com.mganaly.cdmp.data.type.TermProdIdParser;
import com.mganaly.cdmp.data.type.eInvalidCounters;
import com.mganaly.cdmp.data.type.FloatParser;
/**
 * 
 * @author xxx
 * 
 * class cdmp_td_aaa_bill_d
 * 
 * Column
 * 
 * 
 * 
 *
 * V20160525
STATIS_DAY		20120101
SERV_NUMBER		15832015849
BILL_TIME		20120101080603
CP_ID			699004
SERVICE_TYPE	2
CHARGE_TYPE		1
BILL_FEE		200
PRODUCT_ID		2012172800
CONTENT_ID		2038375427
USE_DUR			-1.00
USE_FLOW		0.00
OPT_TYPE		0
CHN_ID			0000
CHN_ID_SOURCE	0_30010009916
BROADCAST_TYPE	2
NET_TYPE		1
PROGRAM_ID		500680413
PROGRAM_FATHER_ID	10042325
TERM_PROD_ID	P0000001
BUSINESS_ID		B6000100075
SUB_BUSI_ID		S6000100123
VIRT_BUSI_ID	
PROV_ID			0311
REGION_ID		0310
NEED_FEE	
FREE_TYPE	
EXTERNAL_PRODUCT	
SALES_ID	
VERSION_ID	
CHN_99000	
CHN_ID_NEW		30010009916
BROADCAST_TYPE_NEW	
DISCOUNT_TYPE	
BROADCAST_START_TIME	
BROADCAST_END_TIME	
FIRST_ORDER_FLAG	
LOG_SOURCE	
NEW_PRODUCT_ID
SESSION_ID	
PAGE_ID	
POSE_ID	
IMEI
 * 
 *
 */
public class td_aaa_bill_d extends cdmp_base_tbl {
	
	private static final Log _LOG = LogFactory.getLog(td_aaa_bill_d.class);
	
	/*
	 * 1.2.2	表结构差异
			序号	字段名	FI	CDMP
			1	STATIS_DAY	√	√
			..….	..….	..….	..….
			19	INSERT_DAY	╳	√
			..….	..….	..….	..….
			40	LOG_SOURCE_NEW	╳	√
			41	BUREAU_DATA_ID	╳	√

	 */

	private static enum e_td_aaa_bill_d {
		STATIS_DAY(0)
		, SERV_NUMBER(1)
		, BILL_TIME(2)
		, CP_ID (3)
		, CHARGE_TYPE (5)
		, BILL_FEE (6)
		, PRODUCT_ID (7)		 
		, CONTENT_ID(8)
		, USE_DUR(9)
		, USE_FLOW(10)
		, OPT_TYPE (11)
		, PROGRAM_ID(16)
		, PROGRAM_FATHER_ID (17)
		, TERM_PROD_ID(18)
		, PROV_ID (22)
		, REGION_ID (23)
		, CHN_ID_NEW(29)
		, PAGE_ID(39)
		, POSE_ID(40)
		, LENGTH(42);

		private int iCode = 0;

		private e_td_aaa_bill_d(int code) {
			this.iCode = code;
		}

		public final int v() {
			return this.iCode;
		}
	} // static enum e_td_aaa_bill_d

	

	public td_aaa_bill_d() {
		
	}
	
	@Override
	protected int getIdx(String colName) {
		
		e_td_aaa_bill_d[] eCols = e_td_aaa_bill_d.values();
		for (e_td_aaa_bill_d eCol : eCols) {
			
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

			if (idx == e_td_aaa_bill_d.STATIS_DAY.v()) {
				newParser = new STATIS_DAY_Parser(colName);
			} else if (idx == e_td_aaa_bill_d.SERV_NUMBER.v()) {
				newParser = new SERV_NUMBER_Parser(colName);
			} else if (idx == e_td_aaa_bill_d.BILL_TIME.v()) {
				newParser = new BILL_TIME_Parser(colName);
			} else if (idx == e_td_aaa_bill_d.CP_ID.v()) {
				newParser = new td_CP_ID_Parser(colName);
			} else if (idx == e_td_aaa_bill_d.CHARGE_TYPE.v()) {
				newParser = new CHARGE_TYPE_Parser(colName);
			} else if (idx == e_td_aaa_bill_d.BILL_FEE.v()) {
				newParser = new BILL_FEE_Parser(colName);
			} else if (idx == e_td_aaa_bill_d.PRODUCT_ID.v()) {
				newParser = new PRODUCT_ID_Parser(colName);
			} else if (idx == e_td_aaa_bill_d.OPT_TYPE.v()) {
				newParser = new OPT_TYPE_Parser(colName);
			} else if (idx == e_td_aaa_bill_d.CONTENT_ID.v()) {
				newParser = new CONTENT_ID_Parser(colName);
			} else if (idx == e_td_aaa_bill_d.USE_DUR.v()) {
				newParser = new USE_DUR_Parser(colName);
			} else if (idx == e_td_aaa_bill_d.USE_FLOW.v()) {
				newParser = new USE_FLOW_Parser(colName);
			} else if (idx == e_td_aaa_bill_d.PROGRAM_ID.v()) {
				newParser = new PROGRAM_ID_Parser(colName);
			} else if (idx == e_td_aaa_bill_d.PROGRAM_FATHER_ID.v()) {
				newParser = new PROGRAM_FATHER_ID_Parser(colName);
			} else if (idx == e_td_aaa_bill_d.TERM_PROD_ID.v()) {
				newParser = new TERM_PROD_ID_Parser(colName);
			} else if (idx == e_td_aaa_bill_d.PROV_ID.v()) {
				newParser = new PROV_ID_Parser(colName);
			} else if (idx == e_td_aaa_bill_d.REGION_ID.v()) {
				newParser = new REGION_ID_Parser(colName);
				
			} else if (idx == e_td_aaa_bill_d.CHN_ID_NEW.v()) {
				newParser = new CHN_ID_NEW_Parser(colName);
			} else if (idx == e_td_aaa_bill_d.PAGE_ID.v()) {
				newParser = new PAGE_ID_Parser(colName);
			} else if (idx == e_td_aaa_bill_d.POSE_ID.v()) {
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
		return e_td_aaa_bill_d.LENGTH.v();
	}
	

	/*
	 * Public class STATIS_DAY_Parser
	 */

	protected static class STATIS_DAY_Parser extends StatisDayParser {


		public STATIS_DAY_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_aaa_bill_d.STATIS_DAY.v();
		}

	}
	
	/*
	 * class SERV_NUMBER_Parser
	 */
	protected static class SERV_NUMBER_Parser extends SvrNumParser {

		public SERV_NUMBER_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_aaa_bill_d.SERV_NUMBER.v();
		}
	}
	

	/*
	 * class BILL_TIME_Parser
	 */
	protected static class BILL_TIME_Parser extends DecimalParser {

		public BILL_TIME_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_aaa_bill_d.BILL_TIME.v();
		}
		

		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_BILL_TIME;
		}
		
		protected boolean isDataClean(String strVal) {
			return isInBits(strVal, 14);
		}
	}
	

	/*
	 * class td_CP_ID_Parser
	 * 
	 */
	protected static class td_CP_ID_Parser extends CP_ID_Parser {
		

		public td_CP_ID_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_aaa_bill_d.CP_ID.v();
		}
		
	}

	/*
	 * class CHARGE_TYPE_Parser
	 */
	protected static class CHARGE_TYPE_Parser extends DecimalParser {

		public CHARGE_TYPE_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_aaa_bill_d.CHARGE_TYPE.v();
		}
		

		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_CHARGE_TYPE;
		}
		
		protected boolean isDataClean(String strVal) {
			return isInRange(strVal, 1, 3);
		}
	}
	

	/*
	 * class BILL_FEE_Parser
	 */
	protected static class BILL_FEE_Parser extends DecimalParser {

		public BILL_FEE_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_aaa_bill_d.BILL_FEE.v();
		}
		

		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_BILL_FEE;
		}
		
		protected boolean isDataClean(String strVal) {
			return isInRange(strVal, 1, 10);
		}
	}
	


	/*
	 * class PRODUCT_ID_Parser
	 * 
	 */
	protected static class PRODUCT_ID_Parser extends ProductIdParser {
		

		public PRODUCT_ID_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_aaa_bill_d.PRODUCT_ID.v();
		}

	}
	

	/*
	 * class OPT_TYPE_Parser
	 */
	protected static class OPT_TYPE_Parser extends DecimalParser {

		public OPT_TYPE_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_aaa_bill_d.OPT_TYPE.v();
		}
		
		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_OPT_TYPE;
		}
		
		protected boolean isDataClean(String strVal) {
			return isInRange(strVal, 1, 3);
		}
	}
	
	/*
	 * class CONTENT_ID_Parser
	 */
	protected static class CONTENT_ID_Parser extends ContentIdParser {
		
		public CONTENT_ID_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_aaa_bill_d.CONTENT_ID.v();
		}
	}


	/*
	 * class USE_DUR_Parser
	 */
	protected static class USE_DUR_Parser extends FloatParser {
		
		public USE_DUR_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_aaa_bill_d.USE_DUR.v();
		}
		
		@Override
		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_USE_DUR;
		}
	}


	/*
	 * class USE_FLOW_Parser
	 */
	protected static class USE_FLOW_Parser extends FloatParser {
		public USE_FLOW_Parser(String name) {
			super(name);
		}

		@Override
		public int getIndex() {
			return e_td_aaa_bill_d.USE_FLOW.v();
		}
		
		@Override
		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_USE_FLOW;
		}
	}

	/*
	 * class PROGRAM_ID_Parser
	 * 
	 */
	protected static class PROGRAM_ID_Parser extends ProgramIdParser {
		

		public PROGRAM_ID_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_aaa_bill_d.PROGRAM_ID.v();
		}
	}
	
	

	/*
	 * class PROGRAM_FATHER_ID_Parser
	 * 
	 */
	protected static class PROGRAM_FATHER_ID_Parser extends ProgramIdParser {
		

		public PROGRAM_FATHER_ID_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_aaa_bill_d.PROGRAM_FATHER_ID.v();
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
			return e_td_aaa_bill_d.TERM_PROD_ID.v();
		}
	}

	/*
	 * class PROV_ID_Parser
	 * 
	 */
	protected static class PROV_ID_Parser extends ColParser {
		

		public PROV_ID_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_aaa_bill_d.PROV_ID.v();
		}
		
		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_PROV_ID;
		}

		@Override
		protected boolean isDataClean(String data) {
			// TODO Auto-generated method stub
			return true;
		}
	}

	/*
	 * class REGION_ID_Parser
	 * 
	 */
	protected static class REGION_ID_Parser extends ColParser {
		

		public REGION_ID_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_aaa_bill_d.REGION_ID.v();
		}
		
		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_REGION_ID;
		}

		@Override
		protected boolean isDataClean(String data) {
			// TODO Auto-generated method stub
			return true;
		}
	}

	/*
	 * class CHN_ID_NEW_Parser
	 */
	protected static class CHN_ID_NEW_Parser extends ChnIdNewParser {
		

		public CHN_ID_NEW_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_aaa_bill_d.CHN_ID_NEW.v();
		}

	} // public static class CHN_ID_NEW_Parser

	

	/*
	 * Public class PAGE_ID_Parser 
	 */
	
	protected static class PAGE_ID_Parser extends PageIdParser {

		public PAGE_ID_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_aaa_bill_d.PAGE_ID.v();
		}
	} 

	/*
	 * Public class POSE_ID_Parser 
	 */
	
	protected static class POSE_ID_Parser extends PageIdParser {
		

		public POSE_ID_Parser(String name) {
			super(name);
		}

		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_POSE_ID;
		}

		public int getIndex() {
			return e_td_aaa_bill_d.POSE_ID.v();
		}
		
	}
	
} //class cdmp_td_aaa_bill_d
