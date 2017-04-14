package com.mganaly.cdmp.data.tbl;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mganaly.cdmp.data.type.AllParser;
import com.mganaly.cdmp.data.type.AvaDaylyParser;
import com.mganaly.cdmp.data.type.SumParser;
import com.mganaly.cdmp.data.type.ColArrayParser;
import com.mganaly.cdmp.data.type.ColIdxParser;
import com.mganaly.cdmp.data.type.ColNameParser;
import com.mganaly.cdmp.data.type.ColParser;
import com.mganaly.cdmp.data.type.DataParserCfg;
import com.mganaly.cdmp.data.type.DateParser;
import com.mganaly.cdmp.data.type.FilterComparatorParser;
import com.mganaly.cdmp.data.type.NumParser;
import com.mganaly.cdmp.data.type.NvlParser;
import com.mganaly.cdmp.data.type.RegularExpParser;
import com.mganaly.cdmp.data.type.ReplaceStringParser;
import com.mganaly.cdmp.data.type.StringParser;
import com.mganaly.cdmp.data.type.SumCntParser;
import com.mganaly.cdmp.data.type.CntDistinctParser;

/**
 * 
 * @author xxx
 * 
 * class cdmp_dw
 *
 */
public class cdmp_base_tbl {
	private static final Log _LOG = LogFactory.getLog(cdmp_base_tbl.class);

	
	public int length() {
		return -1;
	}
	
	
	protected ColParser getParser(String group, String colName) throws IOException {
		ColParser newParser = null;
		
		if (0 == group.compareToIgnoreCase(DataParserCfg.PARSER_TYPE_COUNTER)) {
			newParser = new CntDistinctParser(colName);	
		} else if (0 == group.compareToIgnoreCase(DataParserCfg.PARSER_TYPE_NUMBER)) {
				newParser = new NumParser(colName);
		} else if (0 == group.compareToIgnoreCase(DataParserCfg.PARSER_TYPE_AVA_DAY)) {
			newParser = new AvaDaylyParser(colName);				
		} 	else if (0 == group.compareToIgnoreCase(DataParserCfg.PARSER_TYPE_FILTER)) {		
			newParser = new FilterComparatorParser(colName);
		}
		else if (0 == group.compareToIgnoreCase(DataParserCfg.PARSER_TYPE_SUM)) {
			newParser = new SumParser(colName);
		}
		else if (0 == group.compareToIgnoreCase(DataParserCfg.PARSER_TYPE_COLINDEX)) {		
			newParser = new ColIdxParser(colName);
		}
		else if (0 == group.compareToIgnoreCase(DataParserCfg.PARSER_TYPE_NVL)) {		
			newParser = new NvlParser(colName);
		}
		else if (0 == group.compareToIgnoreCase(DataParserCfg.PARSER_TYPE_SUM_CNT)) {		
			newParser = new SumCntParser(colName);
		}
		else if (0 == group.compareToIgnoreCase(DataParserCfg.PARSER_TYPE_ALL)) {		
			newParser = new AllParser(colName);
		}
		else if (0 == group.compareToIgnoreCase(DataParserCfg.PARSER_TYPE_REXP)) {		
			newParser = new RegularExpParser(colName);
		}
		else if (0 == group.compareToIgnoreCase(DataParserCfg.PARSER_TYPE_STRING)) {		
			newParser = new StringParser(colName);
		}
		else if (0 == group.compareToIgnoreCase(DataParserCfg.PARSER_TYPE_REPLACE)) {
			newParser = new ReplaceStringParser(colName);
		}
		else if (0 == group.compareToIgnoreCase(DataParserCfg.PARSER_TYPE_COLARR)) {		
			newParser = new ColArrayParser(colName);
		}
		else if (0 == group.compareToIgnoreCase(DataParserCfg.PARSER_TYPE_COL)) {		
			newParser = new ColNameParser(colName);
		}
		else if (0 == group.compareToIgnoreCase(DataParserCfg.PARSER_TYPE_DATE)) {		
			newParser = new DateParser(colName);
		}
		else {
			//throw new IOException("Error : unkown type group: " 
					//+ group + ", column name" + colName);
		}

		return newParser;

	} // public static ColParser getParser
	
	public ColParser getParser(String [] colDef) throws IOException {
		
		ColParser newParser = null;
		if (1 == colDef.length) {
			newParser = getParser(DataParserCfg.PARSER_TYPE_COL, colDef[0]);
		}
		else if (2 == colDef.length){
			newParser = getParser(colDef[0], colDef[1]);			
		}
		else {
			
		}
		return newParser;
	}
	
	public ColParser getParser(String colCfg) throws IOException {
		
		String[] colDef = colCfg.trim().split(DataParserCfg.SUB_SEP_SPLIT);
		
		ColParser newParser = getParser(colDef);
		
		if (null == newParser) {
			throw new IOException(colCfg + " parse failed by " + DataParserCfg.SUB_SEP_SPLIT);
		}
		return newParser;
	}
	
	protected void checkParser (ColParser colParser, 
			String group, String colName) throws IOException {
		
		if (null == colParser) {
			String errMsg = "Unkown type group: " + group + ", column name " + colName;
			_LOG.error(errMsg);
			throw new IOException(errMsg);
		}
	}


	protected int getIdx(String colName) {
		// TODO Auto-generated method stub
		return 0;
	}

} // public class cdmp_dw
