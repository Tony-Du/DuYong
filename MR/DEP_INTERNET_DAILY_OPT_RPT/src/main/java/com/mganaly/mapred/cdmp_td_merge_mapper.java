package com.mganaly.mapred;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.mganaly.cdmp.data.tbl.cdmp_base_tbl;
import com.mganaly.cdmp.data.type.ColParser;
import com.mganaly.conf.GlobalEv;

public class cdmp_td_merge_mapper extends base_mapper {

	@Override
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {


		super.map(key, value, context);

	} // protected void map
	

}
