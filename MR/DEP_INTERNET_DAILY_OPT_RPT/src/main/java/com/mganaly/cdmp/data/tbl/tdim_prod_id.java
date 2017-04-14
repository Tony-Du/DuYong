package com.mganaly.cdmp.data.tbl;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mganaly.cdmp.data.type.ColParser;
import com.mganaly.cdmp.data.type.DataParserCfg;
import com.mganaly.cdmp.data.type.StringParser;
import com.mganaly.cdmp.data.type.TermProdIdParser;
import com.mganaly.cdmp.data.type.eInvalidCounters;
import com.mganaly.cdmp.data.type.ColParser.eDataCleanStrict;


/**
 * 
 * @author xxx
 * 
 * 20160525
TERM_PROD_ID			P0003224
TERM_PROD_NAME			咪咕视频会员版手机客户端_苹果版
TERM_PROD_TYPE_ID		T0000002
TERM_PROD_TYPE_NAME		客户端门户
TERM_PROD_CLASS_ID		C0000001
TERM_PROD_CLASS_NAME	手机
TERM_VERSION_ID			340400
TERM_VIDEO_TYPE_ID		TV00303
TERM_VIDEO_TYPE_NAME	咪咕视频
TERM_VIDEO_SOFT_ID		TST000002
TERM_VIDEO_SOFT_NAME	APP
 *
 *
 */

public class tdim_prod_id extends cdmp_base_tbl {
	
	private static final Log _LOG = LogFactory.getLog(tdim_prod_id.class);
	

	/*
	 * enum eAppType
	 */
	private enum eAppType {
		MG_ShiPin,
		MG_HeShiPin,
		MG_Movie,
		MG_Live
	}

	/*
	 * enum e_td_pub_visit_log_d
	 */
	private static enum e_tdim_prod_id {
		TERM_PROD_ID (0)
		, TERM_PROD_NAME(1)
		, TERM_PROD_TYPE_ID(2)
		, TERM_PROD_TYPE_NAME(3)
		, TERM_PROD_CLASS_ID(4)
		, TERM_PROD_CLASS_NAME(5)
		, TERM_VERSION_ID(6)
		, TERM_VIDEO_TYPE_ID(7)
		, TERM_VIDEO_TYPE_NAME(8)
		, TERM_VIDEO_SOFT_ID(9)
		, TERM_VIDEO_SOFT_NAME(10)
		, LENGTH(11);

		private int iCode = 0;

		private e_tdim_prod_id(int code) {
			this.iCode = code;
		}

		public int v() {
			return this.iCode;
		}
	} // static enum e_td_pub_visit_log_d
	
	public tdim_prod_id() {
	}
	
	/*
	 * P0002221	咪咕视频手机客户端_安卓版
		P0003124	咪咕视频和4G版手机客户端_安卓版
		P0002722	咪咕视频手机客户端HK_安卓版
		P0003126	咪咕视频会员版手机客户端_安卓版
		P0002522	咪咕视频手机客户端_苹果版
		P0003224	咪咕视频会员版手机客户端_苹果版
		P0002723	咪咕视频手机客户端HK_苹果版
		P0002924	咪咕视频Pad客户端_苹果版
	 */


	@Override
	protected int getIdx(String colName) {
		
		e_tdim_prod_id[] eCols = e_tdim_prod_id.values();
		for (e_tdim_prod_id eCol : eCols) {
			
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
			if (idx == e_tdim_prod_id.TERM_PROD_ID.v()) {
				newParser = new TERM_PROD_ID_Parser(colName);
			} else if (idx == e_tdim_prod_id.TERM_PROD_ID.v()) {
				newParser = new TERM_PROD_NAME_Parser(colName);
			} else if (idx == e_tdim_prod_id.TERM_VIDEO_TYPE_ID.v()) {
				newParser = new TERM_VIDEO_TYPE_ID_Parser(colName);
			}
			
		} else if (0 == group.compareToIgnoreCase(DataParserCfg.PARSER_TYPE_APP)) {
			newParser = new AppNameParser(colName);
		} else if (0 == group.compareToIgnoreCase(DataParserCfg.PARSER_TYPE_APPFILTER)) {
			newParser = new AppTypeFilterParser(colName);

		}
		

		if (null == newParser) {
			newParser = super.getParser(group, colName);			
			checkParser(newParser, group, colName);
		}

		return newParser;

	} // public ColParser getParser()

	public int length() {
		return e_tdim_prod_id.LENGTH.v();
	}

	/*
	 * Public class TERM_PROD_ID_Parser
	 */

	protected static class TERM_PROD_ID_Parser extends TermProdIdParser {
	
		public TERM_PROD_ID_Parser(String name) {
			super(name);
			// TODO Auto-generated constructor stub
		}

		@Override
		public int getIndex() {
			return e_tdim_prod_id.TERM_PROD_ID.v();
		}


	}
	
	/*
	 * Public class TERM_PROD_ID_Parser
	 */

	protected static class TERM_PROD_NAME_Parser extends StringParser {
	
		public TERM_PROD_NAME_Parser(String finalVal) {
			super(finalVal);
			// TODO Auto-generated constructor stub
		}

		@Override
		public int getIndex() {
			return e_tdim_prod_id.TERM_PROD_NAME.v();
		}
		
	}
	
	/*
	 * class AppTypeParser
	 */
	private static class AppNameParser extends ColParser {

		private String _appName;

		public AppNameParser(String appName) {
			super(appName);
			translateAppType(appName);
			_cleanData = eDataCleanStrict.CUSTOME;
		}

		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_NULL;
		}

		@Override
		protected boolean isDataClean(String strVal) {
			
			if (isNull(strVal)) {
				return false;
			}

			if (!strVal.contains(_appName)) {
				return false;
			}
			
			_colVal = strVal;

			return (!_colVal.isEmpty());
		}
		
		@Override
		public int getIndex() {
			return e_tdim_prod_id.TERM_PROD_NAME.v();
		}
		
		private void translateAppType (String appName) {

			if (0 == appName.compareToIgnoreCase(eAppType.MG_ShiPin.name())) {
				_appName = "咪咕视频";
				
			}
			else if (0 == appName.compareToIgnoreCase(eAppType.MG_Movie.name())) {
				_appName = "咪咕影院";
				
			}
			else if (0 == appName.compareToIgnoreCase(eAppType.MG_Live.name())) {
				_appName = " 咪咕直播";
				
			}
			else {
				_LOG.error("APP Type is illegal" + appName); 
				_appName = "错误";
			}
		}

	} // AppTypeParser
	
		

	
	/*
	 * class AppTypeParser
	 * 
TERM_PROD_ID	P0000023
TERM_PROD_NAME	i视界手机客户端
TERM_PROD_TYPE_ID	T0000002
TERM_PROD_TYPE_NAME	客户端门户
TERM_PROD_CLASS_ID	C0000001
TERM_PROD_CLASS_NAME	手机
TERM_VERSION_ID	010101
TERM_VIDEO_TYPE_ID	TV00002
TERM_VIDEO_TYPE_NAME	和视界
TERM_VIDEO_SOFT_ID	TST000002
TERM_VIDEO_SOFT_NAME	APP

	 * TERM_VIDEO_TYPE_ID TERM_VIDEO_TYPE_NAME
	 * TV00001 和视频
	 * TV00002 和视界
	 * TV00203 咪咕影院
	 * TV00303 咪咕视频
	 * TV00305 咪咕FM
	 * TV00306 咪咕直播
	 */
	

	/*
	 * Public class TERM_PROD_ID_Parser
	 */

	protected static class TERM_VIDEO_TYPE_ID_Parser extends ColParser {
	
		public TERM_VIDEO_TYPE_ID_Parser(String finalVal) {
			super(finalVal);
		}

		@Override
		public int getIndex() {
			return e_tdim_prod_id.TERM_VIDEO_TYPE_ID.v();
		}
		
		@Override
		protected boolean isDataClean(String strVal) {

			Pattern pattern = Pattern.compile("^TV([0-9]{2,8})$");
			Matcher isValidate = pattern.matcher(strVal);

			return isValidate.matches();
		}

		@Override
		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_TERM_VIDEO_TYPE_ID;
		}

	}
	
	
	/**
	 * 
	 * class AppTypeFilterParser
	 *
	 */
	protected static class AppTypeFilterParser extends ColParser {
		
		private final String SEPRATOR_SPLIT = "\\x7c"; // | 
		
	
		private String [] _appTypes;
	
		public AppTypeFilterParser(String appName) {
			super(appName);
			translateAppType(appName);
		}
	
	
		protected boolean isDataClean(String strVal) {
			
			boolean isVal = false;

			for (String app : _appTypes) {
				if (0 == strVal.compareToIgnoreCase(app)) {
					isVal = true;
					break;
				}
			}

			return isVal;
		}
		
		
	
		private void translateAppType (String appName) {
			
			String [] appTypes = appName.trim().split(SEPRATOR_SPLIT);
			
			_appTypes = new String[appTypes.length];
			
			for (int i = 0; i < appTypes.length; ++i) {
				
				String appType = appTypes[i].trim();
				
				if (0 == appType.compareToIgnoreCase(eAppType.MG_ShiPin.name())) {
					_appTypes[i] = "TV00303";
					
				}
				else if (0 == appType.compareToIgnoreCase(eAppType.MG_HeShiPin.name())) {
					_appTypes[i] = "TV00001";
					
				}
				
				else if (0 == appType.compareToIgnoreCase(eAppType.MG_Movie.name())) {
					_appTypes[i] = "TV00203";
					
				}
				else if (0 == appType.compareToIgnoreCase(eAppType.MG_Live.name())) {
					_appTypes[i] = "TV00306";
					
				}
				else {
					_appTypes[i] = "ERROR";
				}
			}
		}
	

		@Override
		public int getIndex() {
			return e_tdim_prod_id.TERM_VIDEO_TYPE_ID.v();
		}

		@Override
		public eInvalidCounters getCounterId() {
			return eInvalidCounters.INV_TERM_VIDEO_TYPE_ID;
		}
	}
	

} //cdmp_tdim_prod_id
