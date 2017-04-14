package com.mganaly.mapred;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.mganaly.cdmp.data.tbl.cdmp_base_tbl;
import com.mganaly.cdmp.data.type.ColParser;
import com.mganaly.cdmp.data.type.DataParserCfg;

public class cdmp_td_select_mapper extends base_mapper {
	
	private int _expect_data_len = 0;
	

	private static final Log _LOG = LogFactory.getLog(cdmp_td_select_mapper.class);
	 
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		super.setup(context);

		Configuration conf = context.getConfiguration();

		String str_data_len	= conf.get(DataParserCfg.PREPROCESS_DATA_LENGTH);
		if (null == str_data_len) {
			String err_msg = "ERROR: " + DataParserCfg.PREPROCESS_DATA_LENGTH + "is null.";
			_LOG.error(err_msg);
			throw new IOException (err_msg);
		}
		
		_expect_data_len = Integer.parseInt(str_data_len);
		
		if (0 >= _expect_data_len) {
			String err_msg = "ERROR: _expect_data_len is " + _expect_data_len 
					+ ". DataParserCfg.PREPROCESS_DATA_LENGTH is " 
					+ DataParserCfg.PREPROCESS_DATA_LENGTH;
			_LOG.error(err_msg);
			throw new IOException (err_msg);
			
		}

	} // void setup

	@Override
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

		String line = value.toString();
		String [] items = ColParser.splitString(line, _expect_data_len);

        if (null==items) {
        	_cntInvLen.increment(1);
        	_LOG.warn("Split as null while the text value is " + line);
        	return;
        }
        
		super.mapString(items, context);

	} // protected void map
}
