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
import com.mganaly.cdmp.data.type.TermTypeParser;

/**
 * 
 * 
 *
 *
 *
 *V20160530 CDMP 
 * STATIS_DAY 20150801 
 * SEARCH_TIME 20150801092820 
 * SERV_NUMBER 13502274855 
 * NODE_ID 60333687820150801013 
 * SUP_NODE_ID 603336878 
 * URL_DETAIL /iworld//publish/clt/resource/isj2/player/playerData.jsp?contentId=
 * SUP_URL_DETAIL 
 * CHN_ID 0111
 * CHN_ID_SOURCE 0111_32000210-99000-200700280020001
 * CP_ID -1 
 * NODE_TYPE 0
 * NET_TYPE 4 
 * TERM_TYPE huawei_che2-tl00m_android 
 * GATE_IP 163.179.230.237
 * SESSION_ID 2015080109280601199169 
 * PAGE_ID 0000 
 * TERM_PROD_ID P0000023
 * BUSINESS_ID 9999 
 * SUB_BUSI_ID 9999 
 * VIRT_BUSI_ID 9999 
 * PROV_ID 0200 
 * REGION_ID 0752 
 * SALES_ID 
 * VERSION_ID 32000210 
 * CHN_99000 99000 
 * CHN_ID_NEW 200700280020001
 * USER_ID 342436828 
 * USER_TYPE 01 
 * NEW_PRODUCT_ID 
 * SUP_PAGE_ID 
 * SUP_POSE_ID
 * SESSION_STEP 
 * IS_VOMS 0 
 * CLIENT_ID 3425890587799 
 * IMEI 
 * IMEI_NEW 
 * COOKIE
 * CONT_OPER_ATTR_ID
 * 
 */
public class td_pub_visit_log_d extends cdmp_base_tbl {

	private static final Log _LOG = LogFactory.getLog(td_pub_visit_log_d.class);

	/*
	 * enum e_td_pub_visit_log_d
	 */
	private static enum e_td_pub_visit_log_d {
		STATIS_DAY(0)
		, SERV_NUMBER(2)
		, NODE_ID(3)
		, CONTENT_ID(5) // From URL_DETAIL
		, TERM_TYPE(12)
		, GATE_IP(13)
		, PAGE_ID(15)
		, TERM_PROD_ID(16)
		, CHN_ID_NEW(25)
		, USER_ID(26)
		, USER_TYPE(27)
		, SUP_POSE_ID(30)
		, IMEI_NEW(35)
		, LENGTH(38);

		private int iCode = 0;

		private e_td_pub_visit_log_d(int code) {
			this.iCode = code;
		}

		public int v() {
			return this.iCode;
		}
	} // static enum e_td_pub_visit_log_d

	public td_pub_visit_log_d() {
	}

	@Override
	protected int getIdx(String colName) {

		e_td_pub_visit_log_d[] eCols = e_td_pub_visit_log_d.values();
		for (e_td_pub_visit_log_d eCol : eCols) {

			if (0 == colName.compareToIgnoreCase(eCol.name())) {
				return eCol.v();
			}
		}

		return -1;
	} // public static int getIdx(String colName)

	public ColParser getParser(String group, String colName) throws IOException {
		
		ColParser newParser = null;

		if (0 == group.compareToIgnoreCase(DataParserCfg.PARSER_TYPE_COL)) {
			int idx = getIdx(colName);
			if (idx == e_td_pub_visit_log_d.STATIS_DAY.v()) {
				newParser = new STATIS_DAY_Parser(colName);
			} else if (idx == e_td_pub_visit_log_d.SERV_NUMBER.v()) {
				newParser = new SERV_NUMBER_Parser(colName);
			} else if (idx == e_td_pub_visit_log_d.NODE_ID.v()) {
				newParser = new NODE_ID_Parser(colName);
			} else if (idx == e_td_pub_visit_log_d.CONTENT_ID.v()) {
				newParser = new CONTENT_ID_Parser(colName);
			} else if (idx == e_td_pub_visit_log_d.CHN_ID_NEW.v()) {
				newParser = new CHN_ID_NEW_Parser(colName);
			} else if (idx == e_td_pub_visit_log_d.IMEI_NEW.v()) {
				newParser = new IMEI_NEW_Parser(colName);
			} else if (idx == e_td_pub_visit_log_d.GATE_IP.v()) {
				newParser = new GATE_IP_Parser(colName);
			} else if (idx == e_td_pub_visit_log_d.PAGE_ID.v()) {
				newParser = new PAGE_ID_Parser(colName);
			} else if (idx == e_td_pub_visit_log_d.TERM_TYPE.v()) {
				newParser = new TERM_TYPE_Parser(colName);
			} else if (idx == e_td_pub_visit_log_d.TERM_PROD_ID.v()) {
				newParser = new TERM_PROD_ID_Parser(colName);
			} else if (idx == e_td_pub_visit_log_d.USER_ID.v()) {
				newParser = new USER_ID_Parser(colName);
			} else if (idx == e_td_pub_visit_log_d.USER_TYPE.v()) {
				newParser = new USER_TYPE_Parser(colName);
			} else if (idx == e_td_pub_visit_log_d.SUP_POSE_ID.v()) {
				newParser = new SUP_POSE_ID_Parser(colName);
			} 
		}
		
		if (null == newParser) {
			newParser = super.getParser(group, colName);			
			checkParser(newParser, group, colName);
		}
		
		return newParser;

	} // public ColParser getParser()

	public int length() {
		return e_td_pub_visit_log_d.LENGTH.v();
	}

	/*
	 * Public class STATIS_DAY_Parser
	 */

	protected static class STATIS_DAY_Parser extends StatisDayParser {
		public STATIS_DAY_Parser(String name) {
			super(name);
			// TODO Auto-generated constructor stub
		}

		@Override
		public int getIndex() {
			return e_td_pub_visit_log_d.STATIS_DAY.v();
		}

	}

	/*
	 * class SERV_NUMBER_Parser
	 */
	protected static class SERV_NUMBER_Parser extends SvrNumParser {
		public SERV_NUMBER_Parser(String name) {
			super(name);
			// TODO Auto-generated constructor stub
		}

		@Override
		public int getIndex() {
			return e_td_pub_visit_log_d.SERV_NUMBER.v();
		}

	} // public static class td_SvrNumParser
	
	

	/*
	 * class NODE_ID_Parser
	 * 
	 */
	protected static class NODE_ID_Parser extends ProgramIdParser {
		

		public NODE_ID_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_pub_visit_log_d.NODE_ID.v();
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
			return e_td_pub_visit_log_d.CONTENT_ID.v();
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
			
			if (!bSucceed) {
				_counter.increment(1);
			}

			return bSucceed;
		} 

	} // class CONTENT_ID_Parser

	/*
	 * class USER_ID_Parser
	 */
	protected static class GATE_IP_Parser extends GateIpParser {
		public GATE_IP_Parser(String name) {
			super(name);
			// TODO Auto-generated constructor stub
		}

		@Override
		public int getIndex() {
			return e_td_pub_visit_log_d.GATE_IP.v();
		}

	}

	/*
	 * class USER_ID_Parser
	 */
	protected static class USER_ID_Parser extends SvrNumParser {
		public USER_ID_Parser(String name) {
			super(name);
			// TODO Auto-generated constructor stub
		}

		@Override
		public int getIndex() {
			return e_td_pub_visit_log_d.USER_ID.v();
		}

	}

	/*
	 * class UserTypeParser
	 */
	protected static class USER_TYPE_Parser extends SvrNumParser {

		public USER_TYPE_Parser(String name) {
			super(name);
			// TODO Auto-generated constructor stub
		}

		@Override
		public int getIndex() {
			return e_td_pub_visit_log_d.USER_TYPE.v();
		}

		protected boolean isValidate(String strVal) {

			if (isNull(strVal))
				return false;

			Pattern pattern = Pattern.compile("^[0-9]{1,10}$");
			Matcher isValidate = pattern.matcher(strVal);
			return isValidate.matches();
		}

	}

	/*
	 * Public class PAGE_ID_Parser
	 */

	protected static class PAGE_ID_Parser extends PageIdParser {

		public PAGE_ID_Parser(String name) {
			super(name);
			// TODO Auto-generated constructor stub
		}

		@Override
		public int getIndex() {
			return e_td_pub_visit_log_d.PAGE_ID.v();
		}
	}

	/*
	 * Public class STATIS_DAY_Parser
	 */

	protected static class TERM_PROD_ID_Parser extends TermProdIdParser {
		public TERM_PROD_ID_Parser(String name) {
			super(name);
			// TODO Auto-generated constructor stub
		}

		@Override
		public int getIndex() {
			return e_td_pub_visit_log_d.TERM_PROD_ID.v();
		}

	}

	/*
	 * class CHN_ID_NEW_Parser
	 */
	protected static class CHN_ID_NEW_Parser extends ChnIdNewParser {
		public CHN_ID_NEW_Parser(String name) {
			super(name);
			// TODO Auto-generated constructor stub
		}

		@Override
		public int getIndex() {
			return e_td_pub_visit_log_d.CHN_ID_NEW.v();
		}
	}

	/*
	 * class TERM_TYPE_Parser
	 */
	protected static class TERM_TYPE_Parser extends TermTypeParser {
		public TERM_TYPE_Parser(String name) {
			super(name);
			// TODO Auto-generated constructor stub
		}

		@Override
		public int getIndex() {
			return e_td_pub_visit_log_d.TERM_TYPE.v();
		}
	}

	/*
	 * Public class SUP_POSE_ID_Parser
	 */

	protected static class SUP_POSE_ID_Parser extends PageIdParser {
		public SUP_POSE_ID_Parser(String name) {
			super(name);
		}

		@Override
		public int getIndex() {
			return e_td_pub_visit_log_d.SUP_POSE_ID.v();
		}

	}

	/*
	 * Public class IMEI_NEW_Parser
	 */

	protected static class IMEI_NEW_Parser extends IMEIParser {
		public IMEI_NEW_Parser(String name) {
			super(name);
			// TODO Auto-generated constructor stub
		}

		@Override
		public int getIndex() {
			return e_td_pub_visit_log_d.IMEI_NEW.v();
		}

	}

} // class cdmp_td_pub_visit_log_d
