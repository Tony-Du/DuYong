package com.mganaly.cdmp.data.tbl;

import java.io.IOException;
import com.mganaly.cdmp.data.tbl.cdmp_base_tbl;
import com.mganaly.cdmp.data.type.CP_ID_Parser;
import com.mganaly.cdmp.data.type.ColParser;
import com.mganaly.cdmp.data.type.ContentIdParser;
import com.mganaly.cdmp.data.type.DataParserCfg;
import com.mganaly.cdmp.data.type.DecimalParser;
import com.mganaly.cdmp.data.type.eInvalidCounters;

/**
 * 
 * class td_cms_content_d
 * 
cdmp_dw.td_cms_content_d
CONTENT_ID	2033139241
CP_ID	699013
PLAY_TYPE	4
CONTENT_STATUS	2
CREATE_DATE	20110118151206
LAST_MODIFY_TIME	20110118205317
LAST_MODIFY_LOGIN	
CREATE_LOGIN	1887
PROV_ID	
CONTENT_NAME	90后军艺车模变装写真
DURATION	26
DETAIL	最美女兵从疯狂到大胆的角色扮演，总共尝试了7种极富魅力的变装…
KEY_WORDS	禁区突击,短兵相接,0118w军事,测试节目勿下线
CONTENT_TYPE	1
ICMS_ID	2033139241
NCP_ID	
UD_ID	
COPYRIGHT_PROVIDER	
IS_ICMS	0
COPYRIGHT_ID	0000000000000001
INSERT_DAY	
CONV_TYPE	
AUTHORIZATION_WAY	
MIGU_PUBLISH	
BC_LICENSE	
IN_FLUENCE	
ORI_PUBLISH	
FIRST_POMS_TIME	
 *
 */
public class td_cms_content_d extends cdmp_base_tbl{
	/*
	 * enum e_td_cms_content_d
	 */
	private static enum e_td_cms_content_d {
		CONTENT_ID(0)
		, CP_ID(1)
		, PLAY_TYPE(2)
		, CONTENT_STATUS(3)
		//CREATE_DATE	20110118151206
		//LAST_MODIFY_TIME	20110118205317
		//LAST_MODIFY_LOGIN	
		//CREATE_LOGIN	1887
		//PROV_ID	
		, CONTENT_NAME(9) //12岁越南女兵网络暴红
		, DURATION(10) //26
		, DETAIL(11)	//最美女兵从疯狂到大胆的角色扮演，总共尝试了7种极富魅力的变装…
		, KEY_WORDS(12) //	禁区突击,短兵相接,0118w军事,测试节目勿下线
		, CONTENT_TYPE(13) //	1
		, ICMS_ID(14)	//2033139241
		//NCP_ID	
		// UD_ID	
		//COPYRIGHT_PROVIDER	
		, IS_ICMS(18)	//0
		, COPYRIGHT_ID(19)	//0000000000000001
		//INSERT_DAY	
		//CONV_TYPE	
		//AUTHORIZATION_WAY	
		//MIGU_PUBLISH	
		//BC_LICENSE	
		//IN_FLUENCE	
		//ORI_PUBLISH	
		//FIRST_POMS_TIME	
		
		, LENGTH(22);

		private int iCode = 0;

		private e_td_cms_content_d(int code) {
			this.iCode = code;
		}

		public int v() {
			return this.iCode;
		}
	} // enum e_td_cms_content_d
	

	@Override
	public int length() {
		return e_td_cms_content_d.LENGTH.v();
	}
	
	@Override
	protected int getIdx(String colName) {

		e_td_cms_content_d [] eCols = e_td_cms_content_d.values();
		for (e_td_cms_content_d eCol : eCols) {

			if (0 == colName.compareToIgnoreCase(eCol.name())) {
				return eCol.v();
			}
		}

		return -1;
	} // getIdx(String colName)
	
	public ColParser getParser(String group, String colName) throws IOException {
		
		ColParser newParser = null;
		
		if (0 == group.compareToIgnoreCase(DataParserCfg.PARSER_TYPE_COL)) {
			int idx = getIdx(colName);
			if (idx == e_td_cms_content_d.CONTENT_ID.v()) {
				newParser = new CONTENT_ID_Parser(colName);
			} else if (idx == e_td_cms_content_d.CP_ID.v()) {
				newParser = new CP_ID_Parser(colName);
			} else if (idx == e_td_cms_content_d.PLAY_TYPE.v()) {
				newParser = new PLAY_TYPE_Parser(colName);
			} else if (idx == e_td_cms_content_d.CONTENT_STATUS.v()) {
				newParser = new CONTENT_STATUS_Parser(colName);
			} else if (idx == e_td_cms_content_d.CONTENT_NAME.v()) {
				newParser = new CONTENT_NAME_Parser(colName);
			} else if (idx == e_td_cms_content_d.DURATION.v()) {
				newParser = new DURATION_Parser(colName);
			} else if (idx == e_td_cms_content_d.DETAIL.v()) {
				newParser = new DETAIL_Parser(colName);
			} else if (idx == e_td_cms_content_d.KEY_WORDS.v()) {
				newParser = new KEY_WORDS_Parser(colName);
			} else if (idx == e_td_cms_content_d.CONTENT_TYPE.v()) {
				newParser = new CONTENT_TYPE_Parser(colName);
			} else if (idx == e_td_cms_content_d.ICMS_ID.v()) {
				newParser = new ICMS_ID_Parser(colName);
			} else if (idx == e_td_cms_content_d.IS_ICMS.v()) {
				newParser = new IS_ICMS_Parser(colName);
			} else if (idx == e_td_cms_content_d.COPYRIGHT_ID.v()) {
				newParser = new COPYRIGHT_ID_Parser(colName);
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
			return e_td_cms_content_d.CONTENT_ID.v();
		}
	}
	
	/*
	 * class CP_ID_Parser
	 * 
	 */
	protected static class td_CP_ID_Parser extends CP_ID_Parser {
		

		public td_CP_ID_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_cms_content_d.CP_ID.v();
		}
		
	} //class CP_ID_Parser
	
	

	/*
	 * class PLAY_TYPE_Parser
	 * 
	 */
	protected static class PLAY_TYPE_Parser extends DecimalParser {
		

		public PLAY_TYPE_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_cms_content_d.PLAY_TYPE.v();
		}
		

		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_PLAY_TYPE;
		}

		protected boolean isDataClean(String strVal) {

			return isInRange(strVal, 1, 2);
			
		}
	} //class PLAY_TYPE_Parser
	
	
	/*
	 * class CONTENT_STATUS_Parser
	 * 
	 */
	protected static class CONTENT_STATUS_Parser extends DecimalParser {
		

		public CONTENT_STATUS_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_cms_content_d.CONTENT_STATUS.v();
		}
		

		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_CONTENT_STATUS;
		}

		protected boolean isDataClean(String strVal) {

			return isInRange(strVal, 1, 2);
		}
	} //class CONTENT_STATUS_Parser
	
	
	
	/*
	 * class CONTENT_NAME_Parser
	 * 
	 */
	protected static class CONTENT_NAME_Parser extends ColParser {
		

		public CONTENT_NAME_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_cms_content_d.CONTENT_NAME.v();
		}
		
		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_PROGRAM_NAME;
		}

		@Override
		protected boolean isDataClean(String data) {
			// TODO Auto-generated method stub
			return true;
		}
		
	} //class PROGRAM_NAME_Parser
	

	/*
	 * class DURATION_Parser
	 * 
	 */
	protected static class DURATION_Parser extends DecimalParser {
		

		public DURATION_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_cms_content_d.DURATION.v();
		}
		

		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_DURATION;
		}

		protected boolean isDataClean(String strVal) {
			return isInRange(strVal, 1, 8);
		}
	} //class DURATION_Parser
	

	/*
	 * class DETAIL_Parser
	 * 
	 */
	protected static class DETAIL_Parser extends ColParser {
		

		public DETAIL_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_cms_content_d.DETAIL.v();
		}
		
		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_DETAIL;
		}

		@Override
		protected boolean isDataClean(String data) {
			// TODO Auto-generated method stub
			return true;
		}
		
	} //class DETAIL_Parser
	

	/*
	 * class KEY_WORDS_Parser
	 * 
	 */
	protected static class KEY_WORDS_Parser extends ColParser {
		

		public KEY_WORDS_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_cms_content_d.KEY_WORDS.v();
		}
		
		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_KEY_WORDS;
		}

		@Override
		protected boolean isDataClean(String data) {
			// TODO Auto-generated method stub
			return true;
		}
		
	} //class KEY_WORDS_Parser
	

	/*
	 * class CONTENT_TYPE_Parser
	 * 
	 */
	protected static class CONTENT_TYPE_Parser extends DecimalParser {
		

		public CONTENT_TYPE_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_cms_content_d.CONTENT_TYPE.v();
		}
		

		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_CONTENT_TYPE;
		}

		protected boolean isDataClean(String strVal) {
			return isInRange(strVal, 1, 2);
		}
	} //class DURATION_Parser

	/*
	 * class ICMS_ID_Parser
	 * 
	 */
	protected static class ICMS_ID_Parser extends DecimalParser {
		

		public ICMS_ID_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_cms_content_d.ICMS_ID.v();
		}
		

		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_ICMS_ID;
		}

		protected boolean isDataClean(String strVal) {
			return isInRange(strVal, 8, 12);
		}
	} //class ICMS_ID_Parser
	
	/*
	 * class IS_ICMS_Parser
	 * 
	 */
	protected static class IS_ICMS_Parser extends DecimalParser {
		

		public IS_ICMS_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_cms_content_d.IS_ICMS.v();
		}
		

		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_IS_ICMS;
		}

		protected boolean isDataClean(String strVal) {
			return isInRange(strVal, 1, 2);
		}
	} //class IS_ICMS_Parser
	

	/*
	 * class COPYRIGHT_ID_Parser
	 * 
	 */
	protected static class COPYRIGHT_ID_Parser extends DecimalParser {
		

		public COPYRIGHT_ID_Parser(String name) {
			super(name);
		}

		public int getIndex() {
			return e_td_cms_content_d.COPYRIGHT_ID.v();
		}
		
		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_COPYRIGHT_ID;
		}

		protected boolean isDataClean(String strVal) {
			return isInRange(strVal, 5, 60); //7|30
		}
	} //class COPYRIGHT_ID_Parser
}
