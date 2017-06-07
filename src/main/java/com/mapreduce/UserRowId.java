package com.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class UserRowId extends Configured implements Tool {
	
	public static enum MATCH_COUNTER{
		ROW_ID
	};
	
	public static class  RowIdMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		public void map(LongWritable key, Text value, Context ctx) throws IOException,InterruptedException{
			
			ctx.getCounter(MATCH_COUNTER.ROW_ID).increment(1);
			Long seq = ctx.getCounter(MATCH_COUNTER.ROW_ID).getValue();
			ctx.write(new Text(seq.toString()), new Text(value));	
			
		}
	}

	@Override
	public int run(String[] arg0) throws Exception {

		Configuration conf = new Configuration();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");

		Job job = Job.getInstance(conf);

		
		FileInputFormat.setInputPaths(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		
		
		job.setJobName("Row Num");
		job.setJarByClass(UserRowId.class);

		job.setMapperClass(RowIdMapper.class);
		job.setSortComparatorClass(LongWritable.DecreasingComparator.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
			
		
		return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String...args) throws Exception {
		int status = ToolRunner.run(new UserRowId(), args);
		System.exit(status);
	}
	
	

	

}
