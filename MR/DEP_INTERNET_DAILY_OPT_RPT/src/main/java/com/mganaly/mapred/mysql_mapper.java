package com.mganaly.mapred;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class mysql_mapper extends Mapper<LongWritable,Text,Text,Text>
{  
    public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException 
    {  
    	context.write(new Text(key.toString()), value);
    } 

}
