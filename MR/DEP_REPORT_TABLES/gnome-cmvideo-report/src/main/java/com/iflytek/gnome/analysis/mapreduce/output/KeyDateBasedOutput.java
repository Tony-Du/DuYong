package com.iflytek.gnome.analysis.mapreduce.output;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.iflytek.avro.mapreduce.output.AvroPairOutputFormat;
import com.iflytek.avro.mapreduce.output.MultipleOutputs;

/**
 * 如果想要使用该类，需要在key的后面加上日期，并以'|'分隔
 * @author lzwang2
 *
 */
public class KeyDateBasedOutput extends FileOutputFormat<String, Object>
{
	@Override
	public RecordWriter<String, Object> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException
	{
		final MultipleOutputs mos = new MultipleOutputs(job);
		return new RecordWriter<String, Object>()
		{
			@Override
			public void write(String key, Object value) throws IOException, InterruptedException
			{
				String[] pos = key.split("\\|");
				mos.write(AvroPairOutputFormat.class, pos[1], pos[0], value);
			}

			@Override
			public void close(TaskAttemptContext context) throws IOException, InterruptedException
			{
				mos.close();
			}
		};
	}
}
