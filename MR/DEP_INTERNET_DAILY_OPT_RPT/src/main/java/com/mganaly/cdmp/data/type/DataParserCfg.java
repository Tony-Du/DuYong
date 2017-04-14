package com.mganaly.cdmp.data.type;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.mganaly.cdmp.data.tbl.cdmp_base_tbl;
/**
 * class cdmp_parser_conf 
 * 
 */
public class DataParserCfg {

	private static final Log _LOG = LogFactory.getLog(DataParserCfg.class);
	
	public static final int 	CONFIG_LEN 				= 2;
	public static final String 	SEPRATOR 			= ","; //,comma
	public static final String 	SEPRATOR_SPLIT 		= "\\x2c"; //,comma
	public static final String 	SUB_SEP_SPLIT 		= "\\x3a"; //:semicolon
	public static final String 	COL_DEF_SEP 		= "\\x20"; //space
	
	// Common configuration
	public final static String MAP_INPUT_COLS_CONF_NAME 	= "mg.mapreduce.keyval.map.input.cols";
	public final static String MAP_KEY_CONF_NAME 			= "mg.mapreduce.keyval.map.key";
	public final static String MAP_VAL_CONF_NAME 			= "mg.mapreduce.keyval.map.val";
	public final static String RED_KEY_CONF_NAME 			= "mg.mapreduce.keyval.red.key";
	public final static String RED_VAL_CONF_NAME 			= "mg.mapreduce.keyval.red.val";
	
	public final static String SEPRATOR_SOURCE 				= "mg.mapreduce.keyval.seprator.source";
	public final static String SEPRATOR_TARGET 				= "mg.mapreduce.keyval.seprator.target";
	
	public final static String MR_START_DAY				 	= "mg.mapreduce.keyval.start.day";
	public final static String MR_FROM_DAY				 	= "mg.mapreduce.keyval.from.day";
	

	// join configuration
	public final static String MAP_RIGHT_INPUT_COLS_CONF_NAME 	= "mg.mapreduce.keyval.map.right.input.cols";
	public final static String TD_JOIN_LEFT_FLAG				= "mg.mapreduce.td_join.map.left.flag";
	public final static String TD_JOIN_MAP_RIGHT_KEY_CONF 		= "mg.mapreduce.td_join.map.right.key";
	public final static String TD_JOIN_MAP_RIGHT_VAL_CONF 		= "mg.mapreduce.td_join.map.right.val";
	public final static String TD_JOIN_RED_RIGHT_KEY_CONF 		= "mg.mapreduce.td_join.red.right.key";
	public final static String TD_JOIN_RED_RIGHT_VAL_CONF 		= "mg.mapreduce.td_join.red.right.val";
	

	public final static String JOIN_L 		= "L";
	public final static String JOIN_R 		= "R";
	
	// ------------------------------------------------------------
	// Data Parameters
	public final static String PREPROCESS_DATA_LENGTH			= "mg.mapreduce.preprocess.data.length";
	
	// ------------------------------------------------------------
	// Parser type
	public final static String PARSER_TYPE_COL				= "COL";
	public final static String PARSER_TYPE_DATE				= "DATE";
	public final static String PARSER_TYPE_COUNTER			= "CNT";
	public final static String PARSER_TYPE_AVA_DAY			= "AVADAY";
	public final static String PARSER_TYPE_NUMBER			= "NUM";
	public final static String PARSER_TYPE_FILTER			= "FILTER";
	public final static String PARSER_TYPE_SUM				= "SUM";
	public final static String PARSER_TYPE_COLINDEX			= "IDX";
	public final static String PARSER_TYPE_NVL				= "NVL";
	public final static String PARSER_TYPE_SUM_CNT			= "SUMCNT";
	public final static String PARSER_TYPE_ALL				= "ALL";
	public final static String PARSER_TYPE_APP				= "APP";
	public final static String PARSER_TYPE_APPFILTER		= "APPFILTER";
	public final static String PARSER_TYPE_STRING			= "STRING"; 	//syntax: STRING:val
	public final static String PARSER_TYPE_REXP				= "REXP"; 		// Regular express	
	public final static String PARSER_TYPE_COLARR			= "COLARR"; 	// COLARR:startIndex(begin with 0)_endIndex
	public final static String PARSER_TYPE_REPLACE			= "REPLACE"; 	//syntax: REPLACE:replaceIndex_replaceString_RegularExpress[OPT]_times[OPT]
																			// if replace times as -1, the times is unlimited
	public final static String PARSER_TYPE_COLNAME			= "COL";
	// ------------------------------------------------------------
	// Sub parser type
	
	public static void genCounters(org.apache.hadoop.mapreduce.Mapper.Context context,
			ArrayList<ColParser> parserList) throws IOException {

		for (ColParser cparser : parserList) {

			cparser.getCounter(context);

		}

	} // void setup
	
	public static void genCounters(org.apache.hadoop.mapreduce.Reducer.Context context,
			ArrayList<ColParser> parserList) throws IOException {

		for (ColParser cparser : parserList) {

			cparser.getCounter(context);

		}

	} // void setup
	
	
	public static void parseKVConf(String cfg_val, ArrayList<ColParser> parserList, 
			cdmp_base_tbl data_type) throws IOException {
		
		// Get key and value configured in configuration
		String[] cfg_cols = cfg_val.trim().split(SEPRATOR_SPLIT);
		if (cfg_cols.length == 0) {
			throw new IOException("ERROR: " + cfg_val + "split " 
							+ SEPRATOR_SPLIT + "failed");
		}

		// --------------------------------------------------------
		// Parse key
		// --------------------------------------------------------
		for (String cfg_col : cfg_cols) {
			
			parserList.add(data_type.getParser(cfg_col));			

		} // for (String keyname : keynames)

	} // void setup
	
	
	public static void parseNamedCfg(String cfg_val, ArrayList<ColParser> parserList, 
			cdmp_base_tbl data_type) throws IOException {

		if (null == cfg_val)
			return;
		
		// Get key and value configured in configuration
		String[] cfg_cols = cfg_val.trim().split(SEPRATOR_SPLIT);
		if (cfg_cols.length == 0) {
			throw new IOException("ERROR: " + cfg_val + "split " 
							+ SEPRATOR_SPLIT + "failed");
		}

		// --------------------------------------------------------
		// Parse key
		// --------------------------------------------------------
		int idx = 0;
		for (String cfg_col : cfg_cols) {

			String[] expresses = cfg_col.trim().split(SUB_SEP_SPLIT);

			if (expresses.length > 2) {
				throw new IOException("ERROR: " + cfg_val + ", sub Cfg " 
							+ cfg_col + " failed by "+ SUB_SEP_SPLIT);
			}

			ColParser newParser = new ColNameParser(expresses[expresses.length -1]);
			newParser.setIndex(idx++);
			parserList.add(newParser);

		} // for (String keyname : keynames)

	} // void parseNamedCfg
	

	public static ArrayList<String> parseColsName(String cfg_val) throws IOException {
		
		ArrayList<String> colNameList = new ArrayList<String>();
		
		// Get key and value configured in configuration
		String[] cfg_cols = cfg_val.trim().split(SEPRATOR_SPLIT);
		
		// --------------------------------------------------------
		// Parse column names
		// --------------------------------------------------------
		for (String cfg_col : cfg_cols) {

			String[] expresses = cfg_col.trim().split(SUB_SEP_SPLIT);

			if (expresses.length > 2) {
				throw new IOException("ERROR: " + cfg_val + ", sub Cfg " 
							+ cfg_col + " failed by "+ SUB_SEP_SPLIT);
			}
			
			String colDef = expresses[expresses.length -1].trim();
			
			String colName = colDef.split(COL_DEF_SEP)[0].trim();
			colNameList.add(colName);

		} // for (String keyname : keynames)
		
		return colNameList;

	} // void parseNamedCfg
	

	
	public static void genIndexsByName (ArrayList<String> colsName, ArrayList<ColParser> dstCols) throws IOException
	{

		for (int ki = 0; ki < dstCols.size(); ++ki) {

			ColParser dstCol = dstCols.get(ki);
			
			for (int i = 0; i < colsName.size(); ++i) {
				
				String inColName = colsName.get(i);
				
				if ((0==inColName.compareToIgnoreCase(dstCol.getColName()))
						&& (-1==dstCol.getIndex())) {
					dstCol.setIndex(i);
					break;
				}
			}
		}
	} //void genParserIndexsByName
	
	
	
	public static void genParserIndexs (ArrayList<ColParser> allColsList, ArrayList<ColParser> dstCols)
	{

		for (int ki = 0; ki < dstCols.size(); ++ki) {

			ColParser dstCol = dstCols.get(ki);
			
			for (int i = 0; i < allColsList.size(); ++i) {
				
				ColParser srcCol = allColsList.get(i);
				String inColName = srcCol.getColName();
				
				if ((0==inColName.compareToIgnoreCase(dstCol.getColName()))
						&& (-1==dstCol.getIndex())) {
					dstCol.setIndex(srcCol.getIndex());
					break;
				}
			}
		}
	} //genParserIndexs
	
	public static void setAsKey (ArrayList<ColParser> keyCols) {
		for (int i = 0; i < keyCols.size(); ++i) {
			keyCols.get(i).setAsKey();
		}		
	}
	

	
	
	public static void genParserIndexs (ArrayList<ColParser> dstCols)
	{
		for (int i = 0; i < dstCols.size(); ++i) {
			dstCols.get(i).setIndex(i);
		}
	}
	
	public static String parseData (String [] items, ArrayList<ColParser> parserList) throws IOException
	{
		String oData = new String();
		
		// ------------------------------------------
		// Parse key

		for (int i = 0; i < parserList.size(); ++i) {
			
			if (!parserList.get(i).inputVal(items)) {
				parserList.get(i).clear();
				_LOG.warn(parserList.get(i).Dump() 
						+ " parse failed " + items[parserList.get(i).getIndex()]);
				return oData;
			}

			oData += parserList.get(i).toString() + ColParser.SEPRATOR;
			parserList.get(i).clear();
		}

		if (! oData.isEmpty()) {
			oData = oData.substring(0, oData.length() - 1);
		}
		
		return oData;
	}
	
	public static String parseData (String line, ArrayList<ColParser> parserList) throws IOException
	{
		String oData = new String();
		
		String[] items = line.split(ColParser.SEPRATOR_SPLIT);
		
		// ------------------------------------------
		// Parse key

		for (int i = 0; i < parserList.size(); ++i) {
			
			if (!parserList.get(i).inputVal(items)) {
				parserList.get(i).clear();
				_LOG.warn(parserList.get(i).Dump() 
						+ " parse failed " + items[parserList.get(i).getIndex()]);
				return null;
			}

			oData += parserList.get(i).toString() + ColParser.SEPRATOR;
			parserList.get(i).clear();
		}

		if (! oData.isEmpty()) {
			oData = oData.substring(0, oData.length() - 1);
		}
		
		return oData;
	}
	
	public static KeyValPair processKVData(String [] items, ArrayList<ColParser> keyParserList, 
			ArrayList<ColParser> valParserList) throws IOException
	{
		KeyValPair kv = new KeyValPair();
		
		// ------------------------------------------
		// Parse key
		for (int i = 0; i < keyParserList.size(); ++i) {
			
			if (!keyParserList.get(i).inputVal(items)) {
				
				keyParserList.get(i).clear();

				_LOG.warn(keyParserList.get(i).Dump() 
						+ " parse failed " + items[keyParserList.get(i).getIndex()]);
				
				return null;
			}

			kv._key += keyParserList.get(i).toString() + ColParser.SEPRATOR;
			keyParserList.get(i).clear();
		}

		if (kv._key.isEmpty()) {
			return kv;
		}
		kv._key = kv._key.substring(0, kv._key.length() - 1);
		
		// ------------------------------------------
		// Parse value

		for (int i = 0; i < valParserList.size(); ++i) {
			
			if (!valParserList.get(i).inputVal(items)) {
				
				valParserList.get(i).clear();

				_LOG.warn(valParserList.get(i).Dump() 
						+ " parse failed " + items[valParserList.get(i).getIndex()]);
				
				return null;
			}

			kv._val += valParserList.get(i).toString() + ColParser.SEPRATOR;
			valParserList.get(i).clear();
		}

		if (!kv._val.isEmpty()) {
			kv._val = kv._val.substring(0, kv._val.length() - 1);
		}
		
		return kv;
	}
	

	public static KeyValPair processKVData(String line, ArrayList<ColParser> keyParserList, 
			ArrayList<ColParser> valParserList) throws IOException
	{
		String[] items = line.split(ColParser.SEPRATOR_SPLIT);
		return processKVData(items, keyParserList, valParserList);
	}
	
	
	public static String dumpParser (ArrayList<ColParser> keyParserList)
	{
		String parsers = "";
		
		for (ColParser colParser : keyParserList) {
			parsers += colParser.Dump() + ColParser.SEPRATOR;
		}
		
		return parsers;
	}
}
