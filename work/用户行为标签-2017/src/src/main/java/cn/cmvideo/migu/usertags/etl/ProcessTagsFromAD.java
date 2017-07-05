package cn.cmvideo.migu.usertags.etl;

import cn.cmvideo.migu.usertags.utils.HDFSFunction;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Created by Smart on 2017/6/19.
 */
public class ProcessTagsFromAD extends Configured implements Tool {
    private static final Logger logger = LoggerFactory.getLogger(ProcessTagsFromAD.class);
    private static final String userTagsDir = "/user/sunflower/encryption/phone/20170523/";  //标签库
    private static String userIdDir = "/user/hadoop/haoming/userid/";     //用户ID集
    private static String outputDir = "/user/hadoop/haoming/tagfromad/20170523/";    //用户标签结果

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "ProcessTagsFromAD");
        job.setJarByClass(ProcessTagsFromAD.class);
        job.setMapperClass(TagMapper.class);
//        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(TagReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPaths(job, userTagsDir);

        Path outputPath = new Path(outputDir + "result/");
        FileOutputFormat.setOutputPath(job, outputPath);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath);
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class TagMapper extends Mapper<Object, Text, Text, Text> {
        Set<String> userIDSet = new HashSet<>();

        @Override
        protected final void setup(Mapper.Context context) throws IOException, InterruptedException {
            //从hdfs读取用户id集
            FileSystem hdfs = FileSystem.get(new Configuration());
            BufferedReader br;
            Set<Path> userIDPath = HDFSFunction.getFiles(new Path(userIdDir));
            for(Path path : userIDPath) {
                br = new BufferedReader(new InputStreamReader(hdfs.open(path)));
                String tempLine;
                while (null != (tempLine = br.readLine())) {
                    userIDSet.add(tempLine);
                }
            }
        }

        @Override
        protected final void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String userid = null;
            String usertags = null;
            String[] vals = value.toString().split(",");
            if (vals.length == 2) {
                userid = vals[0];
                usertags = vals[1];
            } else if (vals.length == 3) {
                userid = vals[0];
                usertags = vals[2];
            } else {
                return;
            }
            if (userid != null && usertags != null && userIDSet.contains(userid)) {
                context.write(new Text(userid), new Text(usertags));
            }
        }

        @Override
        protected final void cleanup(Mapper.Context context) throws IOException, InterruptedException {
        }

    }

    public static class TagReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected final void setup(Reducer.Context context) throws IOException, InterruptedException {
            context.getConfiguration().set("stream.reduce.output.field.separator", "\t");
        }

        @Override
        protected final void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> vals = new LinkedHashSet<>();
            for (Text value : values) {
                for (String s : value.toString().split("\\|")) {
                    if(s != null && s.length() >0) {
                        vals.add(s);
                    }
                }
            }
            StringBuilder sb = new StringBuilder();
            for (String val : vals) {
                sb.append(val).append("|");
            }
            context.write(key, new Text(sb.substring(0, sb.length() - 1)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//        if (otherArgs.length != 2) {
//            System.err.println("usage:");
//            System.err.println("hadoop jar usertags.jar <userDir> <resultDir>");
//            System.exit(2);
//        }

//        inputDir = otherArgs[0];
//        outputDir = otherArgs[1];
        conf.set("USERID_DIR", userIdDir);
        conf.set("OUTPUT_DIR", outputDir);
        conf.set("USERTAGS_DIR", userTagsDir);


        //run mr
        int res = -1;
        try {
            res = ToolRunner.run(conf, new ProcessTagsFromAD(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }

        //exit
        System.exit(res);
    }
}
