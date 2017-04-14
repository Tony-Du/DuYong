package com.iflytek.gnome.analysis.mapreduce.output;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.iflytek.avro.io.UnionData;
import com.iflytek.avro.mapreduce.output.AvroPairOutputFormat;
import com.iflytek.avro.mapreduce.output.MultipleOutputs;

public class CheatOutputFormat extends FileOutputFormat<UnionData, UnionData>
{
	@Override
	public RecordWriter<UnionData, UnionData> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException
	{
		final MultipleOutputs mos = new MultipleOutputs(job);

		return new RecordWriter<UnionData, UnionData>()
		{
			@Override
			public void write(UnionData key, UnionData value) throws IOException, InterruptedException
			{
					mos.write(AvroPairOutputFormat.class, value.datum.getClass().getSimpleName(), key.datum, value.datum);
			}

			@Override
			public void close(TaskAttemptContext context) throws IOException, InterruptedException
			{
				mos.close();
			}
		};
	}
}
