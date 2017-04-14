package com.iflytek.gnome.analysis.mapreduce;

import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MRimei {
	public static class Map extends Mapper<Text, Text, Text, Text>{
		
		@Override
		protected void map(Text key,Text value,Context context) throws IOException, InterruptedException{
			String[] cols = value.toString().split("\n");
			String inputPath = getFilePath(context);
			
			//查找得到的imei  输入路径是否包含20160918路径
			if(inputPath.contains("20160918")){
				 
                if (cols.length < 1) return;  
                  
                String imei = cols[0]; // imei  
  
                context.write(new Text(imei),new Text("a#"+"1"));
                
				
			}
			//总共的imei  输入路径是否包含nouseimei路径
			else if(inputPath.contains("nouseimei")){
				if(cols.length < 2) return;
					
				String imei = cols[0];
				
				context.write(new Text(imei),new Text("b#"+"1"));
			}
		}
		

		// 获取文件路径
	    public static String getFilePath(@SuppressWarnings("rawtypes") Mapper.Context context) throws IOException {
	        InputSplit split = context.getInputSplit();
	        Class<? extends InputSplit> splitClass = split.getClass();
	        FileSplit fileSplit = null;
	        if (splitClass.equals(FileSplit.class)) {
	            fileSplit = (FileSplit) split;
	        } else if (splitClass.getName().equals("org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit")) {
	            try {
	                Method getInputSplitMethod = splitClass.getDeclaredMethod("getInputSplit");
	                getInputSplitMethod.setAccessible(true);
	                fileSplit = (FileSplit) getInputSplitMethod.invoke(split);
	            } catch (Exception e) {
	                e.printStackTrace();
	                throw new IOException("getFilePath fail.");
	            }
	        }

	        return fileSplit.getPath().toString();
	    }
	}
	
	
    public static class Reduce extends Reducer<Text, Text, Text, Text>{ 
	  
	  protected void reduce(Text key,Text value,Context context){
		  
	  }
	  
  }
	
   
    public static void main(String[] args) {
		
	}

}
