package com.mganaly.cdmp.mapred;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import com.mganaly.cdmp.data.type.ColParser;
import com.mganaly.mapred.base_mapper;

public abstract class cdmp_base_mapper extends base_mapper{
	
	private static final Log _LOG = LogFactory.getLog(tdim_prod_id_mapper.class);
	
	@Override
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

		String line = value.toString();
		String [] items = ColParser.splitString(line, getDataType().length());

		if (null==items || 0==items.length) {
        	_cntInvLen.increment(1);
        	_LOG.warn("splitString failed : " + line);
        	return;
        }

		super.mapString(items, context);
	}

}
