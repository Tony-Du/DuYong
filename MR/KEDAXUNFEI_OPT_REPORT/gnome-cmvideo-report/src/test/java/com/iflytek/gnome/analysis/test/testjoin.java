package com.iflytek.gnome.analysis.test;
//
//import java.io.IOException;  
//import java.util.HashMap;  
//import java.util.Iterator;  
//import java.util.Vector;  
//  
//import org.apache.hadoop.io.LongWritable;  
//import org.apache.hadoop.io.Text;  
//import org.apache.hadoop.io.Writable;  
//import org.apache.hadoop.mapred.FileSplit;
//import org.apache.hadoop.mapred.InputSplit;
//import org.apache.hadoop.mapred.JobConf;  
//import org.apache.hadoop.mapred.MapReduceBase;  
//import org.apache.hadoop.mapred.Mapper;  
//import org.apache.hadoop.mapred.OutputCollector;  
//import org.apache.hadoop.mapred.RecordWriter;  
//import org.apache.hadoop.mapred.Reducer;  
//import org.apache.hadoop.mapred.Reporter;
//
//import com.iflytek.gnome.analysis.mobile.dsp.RequestProtocol.Context;  
//  
///** 
// * MapReduce实现Join操作 
// */  
public class testjoin {  
//    public static final String DELIMITER = "\\u000a"; // 字段分隔符 \n换行符 
//      
//    // map过程  
//    public static class MapClass extends MapReduceBase implements  
//            Mapper<LongWritable, Text, Text, Text> {  
//                          
//        public void configure(JobConf job) {  
//            super.configure(job);  
//        }  
//          
//        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output,  
//                Context context) throws IOException, ClassCastException {  
//            // 获取输入文件的全路径和名称  
////        	InputSplit inputSplit=(InputSplit)context.getInputSplit();
////        	String filepath=((FileSplit)inputSplit).getPath().getName();           
//        	String filePath = ((FileSplit)reporter.getInputSplit()).getPath().toString();  
//            // 获取记录字符串  
//            String line = value.toString();  
//            // 抛弃空记录  
//            if (line == null || line.equals("")) return;   
//              
//            // 处理来自总imei的记录  
//            if (filePath.contains("combine_imei_0616_reg.txt")) {  
//                String[] values = line.split(DELIMITER); // 按分隔符分割出字段  
//                if (values.length < 1) return;  
//                  
//                String imei = values[0]; // imei  
//  
//                output.collect(new Text(imei),new Text("a#"+"1"));  
//            }  
//            // 处理来自findimei的记录  
//            else if (filePath.contains("part")) {  
//                String[] values = line.split(","); // 按分隔符分割出字段  
//                if (values.length < 2) return;  
//                  
//                String imei = values[0]; // imei  
//                String id = values[1]; // id  
//              
//                  
//                output.collect(new Text(imei), new Text("b#"+id));  
//            }  
//        }  
//    }  
//      
//    // reduce过程  
//    public static class Reduce extends MapReduceBase  
//            implements Reducer<Text, Text, Text, Text> {  
//        public void reduce(Text key, Iterator<Text> values,  
//                OutputCollector<Text, Text> output, Reporter reporter)  
//                throws IOException {  
//                      
//            Vector<String> vecA = new Vector<String>(); // 存放来自表A的值  
//            Vector<String> vecB = new Vector<String>(); // 存放来自表B的值  
//              
//            while (values.hasNext()) {  
//                String value = values.next().toString();  
//                if (value.startsWith("a#")) {  
//                    vecA.add(value.substring(2));  
//                } else if (value.startsWith("b#")) {  
//                    vecB.add(value.substring(2));  
//                }  
//            }  
//              
//            int sizeA = vecA.size();  
//            int sizeB = vecB.size();  
//              
//            // 遍历两个向量  
//            int i, j;  
//            for (i = 0; i < sizeA; i ++) {  
//                for (j = 0; j < sizeB; j ++) {  
//                    output.collect(key, new Text(vecA.get(i) + DELIMITER +vecB.get(j)));  
//                }  
//            }     
//        }  
//    }  
//      
//    protected void configJob(JobConf conf) {  
//        conf.setMapOutputKeyClass(Text.class);  
//        conf.setMapOutputValueClass(Text.class);  
//        conf.setOutputKeyClass(Text.class);  
//        conf.setOutputValueClass(Text.class);  
////        conf.setInputFormat(ReportInputFormat.class);  
////        conf.setOutputFormat(ReportOutFormat.class); 
//    }  
}