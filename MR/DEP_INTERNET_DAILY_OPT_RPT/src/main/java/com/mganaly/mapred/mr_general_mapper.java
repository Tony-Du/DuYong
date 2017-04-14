package com.mganaly.mapred;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class mr_general_mapper extends base_mapper {

	@Override
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

		
		super.map(key, value, context);

	} // protected void map

}
