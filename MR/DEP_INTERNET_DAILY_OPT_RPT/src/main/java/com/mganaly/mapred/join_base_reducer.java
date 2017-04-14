package com.mganaly.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import com.mganaly.cdmp.data.type.ColParser;
import com.mganaly.cdmp.data.type.DataParserCfg;

public abstract class join_base_reducer extends base_reducer {
	 
	private static final Log _LOG = LogFactory.getLog(join_base_reducer.class);

	protected Text _oNocalVlaues = new Text();
	protected ArrayList<ColParser> _LValParserList = new ArrayList<ColParser>();
	protected ArrayList<ColParser> _RValParserList = new ArrayList<ColParser>();
	protected HashMap<String, Integer> _LNocalcTbl = new HashMap<String, Integer>();
	protected HashMap<String, Integer> _RNocalcTbl = new HashMap<String, Integer>();

	protected String _RcfgValCols = null;
	
	protected String _RcfgMapValCols = null;

	protected long _LTBLValCnt = 0;
	protected long _RTBLValCnt = 0;
	protected boolean _bCalcMode = true;
	

	
	protected abstract boolean dataCollectOK ();
	

	
	protected void readConfig (Context context) {
		super.readConfig(context);
		
		Configuration conf = context.getConfiguration();
		_RcfgMapValCols	= conf.get(DataParserCfg.TD_JOIN_MAP_RIGHT_VAL_CONF).trim();
		
		_RcfgValCols = conf.get(DataParserCfg.TD_JOIN_RED_RIGHT_VAL_CONF);
		if (null == _RcfgValCols || _RcfgValCols.isEmpty()) {
			_RcfgValCols = _RcfgMapValCols;
			_bCalcMode = false;
		}
		else {
			_bCalcMode = true;
			_RcfgValCols = _RcfgValCols.trim();
		}
	}
	

	protected void genKVParsers(Context context) throws IOException {

		super.genKVParsers(context);
		
		//String cfgKVCols = _RcfgKeyCols.trim() + DataParserCfg.SEPRATOR + _RcfgValCols.trim();
		DataParserCfg.parseKVConf(_cfgValCols, _LValParserList, getDataType());
		DataParserCfg.genCounters(context, _LValParserList);
		ArrayList<String> LcolsName = DataParserCfg.parseColsName (_cfgValCols);
		DataParserCfg.genIndexsByName(LcolsName, _LValParserList);
		
		DataParserCfg.parseKVConf(_RcfgValCols, _RValParserList, getDataType());
		DataParserCfg.genCounters(context, _RValParserList);
		ArrayList<String> RcolsName = DataParserCfg.parseColsName (_RcfgValCols);
		DataParserCfg.genIndexsByName(RcolsName, _RValParserList);
	} // genKVParsers


	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {

		_LTBLValCnt = 0;
		_RTBLValCnt = 0;
		
		if (_bCalcMode) {
			reduceCalcMode (values, context);
		}
		else {
			reduceNocalcMode (values, context);
		}
		
		if (dataCollectOK()) {
			
			if (_bCalcMode) {
				outCalcReduce (key, context);
			}
			else  {
				//checkLeftGroupBy(key.toString());
				outNocalcReduce (key, context);
			}
		}
		
		_LNocalcTbl.clear();		
		_RNocalcTbl.clear();
		
	} // void reduce
	
	protected void reduceCalcMode (Iterable<Text> values, Context context) throws IOException {
		
		for (Text val : values) {
			
			String[] vCols = val.toString().trim().split(ColParser.SEPRATOR_SPLIT);
						
			int vLen = vCols.length - 1;
			
			// Left table
			if (DataParserCfg.JOIN_L.equals(vCols[vLen])) {
				
				for (ColParser colParser : _LValParserList) {
					if (!colParser.inputVal(vCols)) {
						_LOG.error(colParser.Dump());
					}
				}
				
				_LTBLValCnt++;
				
			}
			// Right table
			else if (DataParserCfg.JOIN_R.equals(vCols[vLen])) {

				for (ColParser colParser : _RValParserList) {
					if (!colParser.inputVal(vCols)) {
						_LOG.error(colParser.Dump());
					}
				}
				
				_RTBLValCnt++;
				
			}	
		} //for (Text val : values)
		
	}
	
	protected void reduceNocalcMode (Iterable<Text> values, Context context) throws IOException {

		for (Text val : values) {
			String[] vCols = val.toString().trim().split(ColParser.SEPRATOR_SPLIT);
						
			int vLen = vCols.length - 1;

			String outKVCols = "";			
			
			// Left table
			if (DataParserCfg.JOIN_L.equals(vCols[vLen])) {
				

				for (ColParser colParser : _LValParserList) {
					if (!colParser.inputVal(vCols)) {
						_LOG.error(colParser.Dump());
					}
					outKVCols += colParser.toString() + ColParser.SEPRATOR;
					colParser.clear();
				}

				_LNocalcTbl.put(outKVCols.trim(), 0);
				
				_LTBLValCnt++;
				
			}
			// Right table
			else if ( DataParserCfg.JOIN_R.equals(vCols[vLen])) {

				for (ColParser colParser : _RValParserList) {
					if (!colParser.inputVal(vCols)) {
						_LOG.error(colParser.Dump());
					}
					outKVCols += colParser.toString() + ColParser.SEPRATOR;
					colParser.clear();
				}
				
				_RNocalcTbl.put(outKVCols.trim(), 0);
				_RTBLValCnt++;
				
			}
		}		
	}
	
	
	protected void outCalcReduce (Text key, Context context) throws IOException, InterruptedException {
		

		String outRedVal = "";
		for (int i = 0; i < _LValParserList.size(); ++i) {
			ColParser valColParser = _LValParserList.get(i);
			outRedVal += valColParser.toString() + ColParser.SEPRATOR;
			valColParser.clear();
		}

		for (int i = 0; i < _RValParserList.size(); ++i) {
			ColParser valColParser = _RValParserList.get(i);
			outRedVal += valColParser.toString() + ColParser.SEPRATOR;
			valColParser.clear();
		}
		outRedVal = outRedVal.trim();

		context.write(key, new Text(outRedVal));
	}
	
	
	protected void outNocalcReduce(Text key, Context context) 
			throws IOException, InterruptedException {

		if (_LNocalcTbl.isEmpty()) {
			String leftPart = "";
			for (int i = 0; i < _LValParserList.size(); ++i) {
				leftPart += _LValParserList.get(i).toString() + ColParser.SEPRATOR;
				_LValParserList.get(i).clear();
			}
			for (String rightPart : _RNocalcTbl.keySet()) {
				_oNocalVlaues.set(leftPart.trim() + ColParser.SEPRATOR + rightPart.trim());
				context.write(key, _oNocalVlaues);
			}
		}
		else if (_RNocalcTbl.isEmpty()) {
			String rightPart = "";

			for (int i = 0; i < _RValParserList.size(); ++i) {
				rightPart += _RValParserList.get(i).toString() + ColParser.SEPRATOR;
				_RValParserList.get(i).clear();
			}
			for (String leftPart : _LNocalcTbl.keySet()) {
				_oNocalVlaues.set(leftPart.trim() + ColParser.SEPRATOR + rightPart.trim());
				context.write(key, _oNocalVlaues);
			}
		}
		else 
		{
			for (String leftPart : _LNocalcTbl.keySet()) {
				for (String rightPart : _RNocalcTbl.keySet()) {
					_oNocalVlaues.set(leftPart.trim() + ColParser.SEPRATOR + rightPart.trim());
					context.write(key, _oNocalVlaues);
				}
			} // for
		}		

	} //outNocalcReduce
}
