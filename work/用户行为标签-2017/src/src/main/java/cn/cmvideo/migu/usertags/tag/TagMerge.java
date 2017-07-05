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

import java.io.IOException;

/**
 * Created by Smart on 2017/6/28.
 */
public class TagMerge extends Configured implements Tool {
    private static String inputDirA;
    private static String inputDirB;
    private static String outputDir;

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "ProcessTagsFromAD");
        job.setJarByClass(TagMerge.class);
        job.setMapperClass(TagMergeMapper.class);
        job.setReducerClass(TagMergeReducer.class);
        job.setNumReduceTasks(20);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPaths(job, inputDirA);
        FileInputFormat.addInputPaths(job, inputDirB);

        Path outputPath = new Path(outputDir.endsWith("/") ? outputDir + "merge_result/" : outputDir + "/merge_result/");
        FileOutputFormat.setOutputPath(job, outputPath);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath);
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static class TagMergeMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected final void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String[] vals = value.toString().split("\\t");
            if(vals.length >= 1) {
                context.write(new Text(vals[0]), value);
            }
        }
    }

    public static class TagMergeReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected final void setup(Reducer.Context context) throws IOException, InterruptedException {
            context.getConfiguration().set("stream.reduce.output.field.separator", "\t");
        }

        @Override
        protected final void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            TagBean tbm = new TagBean();
            tbm.setPhone(key.toString());
            for(Text value : values) {
                TagBean.getMerge(tbm, TagBean.toTagBean(value.toString()));
            }
            context.write(key, new Text(tbm.toJSON()));
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("usage:");
            System.err.println("hadoop jar usertags.jar cn.cmvideo.migu.usertags.tag.TagMerge <inputDirA> <inputDirB> <resultDir>");
            System.exit(2);
        }

        inputDirA = otherArgs[0];
        inputDirB = otherArgs[1];
        outputDir = otherArgs[2];


        //run mr
        int res = -1;
        try {
            res = ToolRunner.run(conf, new TagMerge(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }

        //exit
        System.exit(res);
    }
}
