package com.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class EmpDriver extends Configured implements Tool {

	public class EmpMapper extends Mapper<Text, Text, LongWritable, Text> {
		public void map(Text key, Text value, Context context) {
			try {
				int sal = Integer.parseInt(value.toString());
				context.write(new LongWritable(sal), key);
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}
	}

	public static class EmpReducer extends
			Reducer<LongWritable, Text, Text, LongWritable> {
		int mCount = 0;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			mCount = 0;
		}

		public void reduce(LongWritable key, Iterable<Text> values,
				Context context) {
			if (mCount < 10) {
				try {
					for (Text value : values) {
						context.write(value, key);
						mCount++;
					}
				} catch (Exception e) {

				}
			}
		}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");

		Job job = Job.getInstance(conf);

		job.setJobName("Top 10 salried employees");
		job.setJarByClass(getClass());

		job.setNumReduceTasks(1);

		job.setMapperClass(EmpMapper.class);
		job.setReducerClass(EmpReducer.class);
		job.setSortComparatorClass(LongWritable.DecreasingComparator.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.setInputPaths(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));

		return job.waitForCompletion(true) ? 1 : 0;
	}

	public static void main(String[] args) throws Exception {
		
		int status = ToolRunner.run(new Configuration(), new EmpDriver(), args);
		System.exit(status);
	}

}