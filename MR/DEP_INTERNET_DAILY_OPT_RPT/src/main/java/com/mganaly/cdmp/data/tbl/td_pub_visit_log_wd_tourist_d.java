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
import com.mganaly.cdmp.data.type.GateIpParser;
import com.mganaly.cdmp.data.type.IMEIParser;
import com.mganaly.cdmp.data.type.PageIdParser;
import com.mganaly.cdmp.data.type.ProgramIdParser;
import com.mganaly.cdmp.data.type.StatisDayParser;
import com.mganaly.cdmp.data.type.SvrNumParser;
import com.mganaly.cdmp.data.type.TermProdIdParser;
import com.mganaly.cdmp.data.type.eInvalidCounters;
/**
STATIS_DAY	20151101
SEARCH_TIME	20151101072009
TOURIST_VISIT_ID	2015110107200919774E4BEED91166C5C03FC75C65F5F4
SERV_NUMBER	0445370537052
NODE_ID	10327631
SUP_NODE_ID	10327631
URL_DETAIL	/wl/hetiyanbao.jsp
SUP_URL_DETAIL	
CHN_ID	1007
CHN_ID_SOURCE	1007_20041302008
CP_ID	-1
NODE_TYPE	1
NET_TYPE	1
TERM_TYPE	Dalvik
GATE_IP	175.151.221.49
SESSION_ID	
PAGE_ID	0000
TERM_PROD_ID	P0000017
BUSINESS_ID	9999
SUB_BUSI_ID	9999
VIRT_BUSI_ID	9999
PROV_ID	0240
REGION_ID	
SALES_ID	
VERSION_ID	
CHN_99000	
CHN_ID_NEW	20041302008
PAGE_REDIRECT_ID	1
NEW_PRODUCT_ID	
IS_VOMS	0
SUP_PAGE_ID	
SUP_POSE_ID	
SESSION_STEP	
IMEI	
IMEI_NEW	
TOURIST_SOURCE	2
CONT_OPER_ATTR_ID
 *
 *
Path = 
/user/hadoop/public/cdmp/td_aaa_tourist_bill_d/
 *
 */

public class td_pub_visit_log_wd_tourist_d  extends cdmp_base_tbl {
	private static final Log _LOG = LogFactory.getLog(td_pub_visit_log_wd_tourist_d.class);
	

	/*
	 * enum e_td_pub_visit_log_wd_tourist_d
	 */
	private static enum e_td_pub_visit_log_wd_tourist_d {
		STATIS_DAY(0)
		, TOURIST_VISIT_ID(2)
		, SERV_NUMBER(3)
		, NODE_ID(4)
		, CONTENT_ID(6) //From URL_DETAIL
		, GATE_IP(14)
		, TERM_TYPE(13)
		, PAGE_ID (16)
		, TERM_PROD_ID(17)
		, CHN_ID_NEW(26)
		, SUP_POSE_ID (31)
		, IMEI_NEW(34)
		, LENGTH(37)
		;

		private int iCode = 0;

		private e_td_pub_visit_log_wd_tourist_d (int code) {
			this.iCode = code;
		}

		public int v() {
			return this.iCode;
		}
	} // static enum e_td_pub_visit_log_d


	public td_pub_visit_log_wd_tourist_d() {
	}

	@Override
	protected int getIdx(String colName) {
		
		e_td_pub_visit_log_wd_tourist_d[] eCols = e_td_pub_visit_log_wd_tourist_d.values();
		for (e_td_pub_visit_log_wd_tourist_d eCol : eCols) {
			
			if (0 == colName.compareToIgnoreCase(eCol.name())) {
				return eCol.v();
			}
		}
		
		return -1;
	} //public static int getIdx(String colName)
	

	public ColParser getParser(String group, String colName) 
			throws IOException {

		ColParser newParser = null;

		if (0 == group.compareToIgnoreCase(DataParserCfg.PARSER_TYPE_COL)) {
			int idx = getIdx(colName);			
			if (idx == e_td_pub_visit_log_wd_tourist_d.STATIS_DAY.v()) {
				newParser = new STATIS_DAY_Parser(colName);
			}else if (idx == e_td_pub_visit_log_wd_tourist_d.TOURIST_VISIT_ID.v()) {
				newParser = new TOURIST_VISIT_ID_Parser(colName);
			}else if (idx == e_td_pub_visit_log_wd_tourist_d.SERV_NUMBER.v()) {
				newParser = new SERV_NUMBER_Parser(colName);
			}else if (idx == e_td_pub_visit_log_wd_tourist_d.NODE_ID.v()) {
				newParser = new NODE_ID_Parser(colName);
			} else if (idx == e_td_pub_visit_log_wd_tourist_d.CONTENT_ID.v()) {
				newParser = new CONTENT_ID_Parser(colName);
			} else if (idx == e_td_pub_visit_log_wd_tourist_d.CHN_ID_NEW.v()) {
				newParser = new CHN_ID_NEW_Parser(colName);
			} else if (idx == e_td_pub_visit_log_wd_tourist_d.GATE_IP.v()) {
				newParser = new GATE_IP_Parser(colName);
			} else if (idx == e_td_pub_visit_log_wd_tourist_d.PAGE_ID.v()) {
				newParser = new PAGE_ID_Parser(colName);
			} else if (idx == e_td_pub_visit_log_wd_tourist_d.TERM_PROD_ID.v()) {
				newParser = new TERM_PROD_ID_Parser(colName);
			} else if (idx == e_td_pub_visit_log_wd_tourist_d.SUP_POSE_ID.v()) {
				newParser = new SUP_POSE_ID_Parser(colName);
			} else if (idx == e_td_pub_visit_log_wd_tourist_d.IMEI_NEW.v()) {
				newParser = new IMEI_NEW_Parser(colName);
			} else {
			} // switch (idx)
		}

		if (null == newParser) {
			newParser = super.getParser(group, colName);			
			checkParser(newParser, group, colName);
		}
		
		return newParser;

	} // public ColParser getParser()
	

	public int length () {
		return e_td_pub_visit_log_wd_tourist_d.LENGTH.v();
	}
	
	/*
	 * Public class STATIS_DAY_Parser 
	 */
	public static class STATIS_DAY_Parser extends StatisDayParser {
		
		public STATIS_DAY_Parser(String name) {
			super(name);
		}

		@Override
		public int getIndex() {
			return e_td_pub_visit_log_wd_tourist_d.STATIS_DAY.v();
		}
		
	} 


	/*
	 * class TOURIST_VISIT_ID_Parser
	 */
	public static class TOURIST_VISIT_ID_Parser extends SvrNumParser {
		
		public TOURIST_VISIT_ID_Parser(String name) {
			super(name);
		}

		@Override
		public int getIndex() {
			return e_td_pub_visit_log_wd_tourist_d.TOURIST_VISIT_ID.v();
		}

		@Override
		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_TOURIST_VISIT_ID;
		}
		
		
		protected boolean isValidate(String strVal) {
			
			if (isNull(strVal))
				return false;
			
			Pattern pattern = Pattern.compile("^[A-Za-z0-9]{12,50}$");
			Matcher isValidate = pattern.matcher(strVal);
			return isValidate.matches();
		}
	}
	

	/*
	 * class SERV_NUMBER_Parser
	 */
	public static class SERV_NUMBER_Parser extends SvrNumParser {
		
		public SERV_NUMBER_Parser(String name) {
			super(name);
		}

		@Override
		public int getIndex() {
			return e_td_pub_visit_log_wd_tourist_d.SERV_NUMBER.v();
		}
		
		protected boolean isValidate(String strVal) {
			
			if (isNull(strVal))
				return false;
			
			Pattern pattern = Pattern.compile("^[A-Za-z0-9]{10,16}$");
			Matcher isValidate = pattern.matcher(strVal);
			return isValidate.matches();
		}
	}
	

	/*
	 * class NODE_ID_Parser
	 * 
	 */
	protected static class NODE_ID_Parser extends ProgramIdParser {
		

		public NODE_ID_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_pub_visit_log_wd_tourist_d.NODE_ID.v();
		}
	}
	


	/*
	 * class CONTENT_ID_Parser
	 */
	protected static class CONTENT_ID_Parser extends ContentIdParser {

		final String _bgPatter = "contentid=";
		
		public CONTENT_ID_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_pub_visit_log_wd_tourist_d.CONTENT_ID.v();
		}
		


		public boolean inputVal(String strUrl) {

			if (strUrl.length() < _bgPatter.length()) {
				return false;
			}

			boolean bSucceed = false;

			int bgIdx = strUrl.toLowerCase().indexOf(_bgPatter);

			if (bgIdx > 0) {

				bgIdx += _bgPatter.length();
				int endIdx = strUrl.indexOf("&", bgIdx);

				if (endIdx <= 0) {
					endIdx = strUrl.indexOf("=", bgIdx);
				}

				String contentId = null;
				if (endIdx <= 0) {
					contentId = strUrl.substring(bgIdx);
				} else {
					contentId = strUrl.substring(bgIdx, endIdx);
				}

				bSucceed = super.inputVal(contentId);
			}

			return bSucceed;
		} 


	} //class CONTENT_ID_Parser

	
	
	
	
	/*
	 * class GATE_IP_Parser
	 */
	public static class GATE_IP_Parser extends GateIpParser {
		public GATE_IP_Parser(String name) {
			super(name);
		}

		@Override
		public int getIndex() {
			return e_td_pub_visit_log_wd_tourist_d.GATE_IP.v();
		}
		
	} 
	
	/*
	 * Public class PAGE_ID_Parser 
	 */
	public static class PAGE_ID_Parser extends PageIdParser {
		public PAGE_ID_Parser(String name) {
			super(name);
		}

		@Override
		public int getIndex() {
			return e_td_pub_visit_log_wd_tourist_d.PAGE_ID.v();
		}
		
	} 

	/*
	 * class CHN_ID_NEW_Parser
	 */
	public static class CHN_ID_NEW_Parser extends ChnIdNewParser {
		public CHN_ID_NEW_Parser(String name) {
			super(name);
		}

		@Override
		public int getIndex() {
			return e_td_pub_visit_log_wd_tourist_d.CHN_ID_NEW.v();
		}
	}
	
	

	/*
	 * Public class SUP_POSE_ID_Parser 
	 */
	public static class SUP_POSE_ID_Parser extends PageIdParser {
		public SUP_POSE_ID_Parser(String name) {
			super(name);
		}

		@Override
		public int getIndex() {
			return e_td_pub_visit_log_wd_tourist_d.SUP_POSE_ID.v();
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
			return e_td_pub_visit_log_wd_tourist_d.TERM_PROD_ID.v();
		}
	}
	

	/*
	 * Public class IMEI_NEW_Parser 
	 */
	protected static class IMEI_NEW_Parser extends IMEIParser {
		public IMEI_NEW_Parser(String name) {
			super(name);
		}

		@Override
		public int getIndex() {
			return e_td_pub_visit_log_wd_tourist_d.IMEI_NEW.v();
		}
	} 
	
} //class cdmp_td_pub_visit_log_wd_tourist_d
