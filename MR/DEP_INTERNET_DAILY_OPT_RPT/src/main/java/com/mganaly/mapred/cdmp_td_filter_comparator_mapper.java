package com.mganaly.mapred;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import com.mganaly.cdmp.data.tbl.cdmp_base_tbl;
import com.mganaly.cdmp.data.type.ColParser;
import com.mganaly.conf.GlobalEv;

public class cdmp_td_filter_comparator_mapper extends base_mapper {
	
	@Override
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

		String line = value.toString().trim();

		String[] items = line.split(ColParser.SEPRATOR);

		if (items.length < (_keyParserList.size() + _valParserList.size())) {
			_cntInvLen.increment(1);
			throw new IOException ("eorr line : " + line);
		}

		super.map(key, value, context);

	} // protected void map

}
