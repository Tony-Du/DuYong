package cn.cmvideo.migu.usertags.tag;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

import static cn.cmvideo.migu.usertags.utils.DateFunction.getDatelabels;

/**
 * Created by Smart on 2017/6/26.
 */
public class ProcessTag  extends Configured implements Tool {
    private static final Logger logger = LoggerFactory.getLogger(ProcessTag.class);

    private static final String srcfileDir = "/user/hadoop/msc/user_use_detail_tag/";  //用户标签原始文件地址
    private static final String outputDir =  "/user/hadoop/usertags/tagresult/";   //用户标签结果数据

    private static String startDate;
    private static String endDate;
    private static Set<String> dateSet;

    public static boolean isDateType(String date) {
        if(date != null && date.length() == 8) {
            try {
                Integer.valueOf(date);
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return false;
    }

    public static String formattedPhoneNum(String phone){
        if(phone.startsWith("+")) { //带+号的手机号，先去掉加号
            phone = phone.substring(1, phone.length());
        }
        if (phone.length() == 11) { //正常手机号
            return phone;
        } else if((phone.length() == 13 ||  phone.length() == 14 ) && phone.substring(0, 3).contains("86")){    //以86开头的手机号
            return phone.substring(phone.length() - 11, phone.length());
        } else if(phone.length() == 12 && phone.startsWith("0")) {  //
            return phone.substring(1, 12);
        } else if (phone.length() == 13 && phone.startsWith("00")) {
            return phone.substring(2, 13);
        } else {
            return "";
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "ProcessTag");
        job.setJarByClass(ProcessTag.class);
        job.setMapperClass(ProcessTagMapper.class);
        job.setReducerClass(ProcessTagReducer.class);
        job.setNumReduceTasks(20);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //
        for(String date : dateSet) {
            if(isDateType(date)) {
                FileInputFormat.addInputPaths(job, (srcfileDir + "src_file_day=" + date));
            }
        }

        Path outputPath = new Path(outputDir + startDate + "_" + endDate);
        FileOutputFormat.setOutputPath(job, outputPath);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath);
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class ProcessTagMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected final void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String[] vals = value.toString().split("\\u0001");
            if(vals.length >= 1) {
                String phone = formattedPhoneNum(vals[0]);  //规则化手机号
                if(phone != null && phone.length() > 0) {
                    context.write(new Text(phone), value);
                }
            }
        }
    }

    public static class ProcessTagReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected final void setup(Reducer.Context context) throws IOException, InterruptedException {
            context.getConfiguration().set("stream.reduce.output.field.separator", "\t");
        }

        @Override
        protected final void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            TagBean tb = new TagBean();
            tb.setPhone(key.toString());
            for(Text value : values) {
                String[] vals = value.toString().split("\\u0001");
                if(vals.length >= 9) {
                    String imei = vals[1];
                    String location = vals[2].replaceAll("\t","|");
                    String class1 = vals[6];
                    String tag1 = vals[7];
                    String keyword = vals[8];

                    tb.imeiCountAddming(imei);
                    tb.locationCountAddming(location);
                    tb.class1CountAddming(class1);
                    tb.tag1CountAddming(tag1);
                    tb.keywordCountAddming(keyword);
                }
            }
            tb.sorted();    //排序
            context.write(key, new Text(tb.toJSON()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2 || !isDateType(otherArgs[0]) || !isDateType(otherArgs[1])) {
            System.err.println("usage:");
            System.err.println("hadoop jar usertags.jar cn.cmvideo.migu.usertags.tag.ProcessTag <startDay> <endDay>");
            System.exit(2);
        }

        startDate = otherArgs[0];
        endDate = otherArgs[1];
        dateSet = getDatelabels(startDate, endDate);

        //run mr
        int res = -1;
        try {
            res = ToolRunner.run(conf, new ProcessTag(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }

        //exit
        System.exit(res);
    }
}
