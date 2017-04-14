package com.mganaly.mapred;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.mganaly.cdmp.data.type.ColParser;
import com.mganaly.cdmp.data.type.DataParserCfg;
import com.mganaly.cdmp.data.type.KeyValPair;

/**
 * 
 * class preprocess_format_mapper
 *
 */
public class prep_seprate_mapper  extends base_mapper {
	
	private static final Log _LOG = LogFactory.getLog(prep_seprate_mapper.class);
	private String _seprator = null;
	
	private final String _NULL = "NULL";
	private int _dataLen = 0;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		super.setup(context);
		
		Configuration conf = context.getConfiguration();		
		_seprator			= conf.get(DataParserCfg.SEPRATOR_SOURCE);
		
		if (null == _seprator || _seprator.isEmpty()) {
			throw new IOException("Empty mg.mapreduce.keyval.seprator.source");
		}
		_dataLen =  colsNames.size();
	} // void setup
	
	@Override
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

		String line = value.toString();
		String [] items = sepSplitString(line, _dataLen);

		if (null==items || 0==items.length) {
        	_cntInvLen.increment(1);
        	_LOG.warn("splitString failed : " + line);

        	return;
        }

		super.mapString(items, context);

	} // protected void map
	
	
	private String[] sepSplitString(String line, int exp_len) throws IOException {

		String[] items = new String[exp_len];
		int start_idx = 0;
		int end_idx = 0;
		int item_idx = 0;
		String item = "";
		while (-1 != (end_idx = line.indexOf(_seprator, start_idx))) {
			item = line.substring(start_idx, end_idx);
			if (item.isEmpty())
				item = _NULL;
			items[item_idx++] = item;
			start_idx = end_idx + 1;
			if (item_idx >= exp_len)
				break;
		}

		if (item_idx != (exp_len - 1)) {
			return null;
		}

		if (start_idx < line.length()) {
			items[item_idx++] = line.substring(start_idx, line.length());
		} else {
			items[item_idx++] = _NULL;
		}

		return items;
	}

}
