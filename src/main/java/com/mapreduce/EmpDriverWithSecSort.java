package com.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class EmpDriverWithSecSort extends Configured implements Tool {

	public static class EmpMapper extends
			Mapper<LongWritable, Text, EmpKey, NullWritable> {

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] emps = value.toString().split(",");
			int empid = Integer.parseInt(emps[0]);
			int sal = Integer.parseInt(emps[1]);
			context.write(new EmpKey(empid, sal), NullWritable.get());
		}
	}

	public static class EmpReducer extends
			Reducer<EmpKey, NullWritable, EmpKey, NullWritable> {
		int mCount = 0;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			mCount = 0;
		}

		public void reduce(EmpKey key, Iterable<NullWritable> values,
				Context context) {
			if (mCount < 10) {
				try {
					context.write(key, NullWritable.get());
					mCount++;
				} catch (Exception e) {

				}
			}
		}
	}
	
	public static class EmpPartitioner  extends Partitioner<EmpKey, NullWritable> {

    @Override
    public int getPartition(EmpKey key, NullWritable value, int numPartitions) {
      return Math.abs(key.getEmpId() * 127) % numPartitions;
    }
  }
  
  public static class EmpKeyComparator extends WritableComparator {
    protected EmpKeyComparator() {
      super(EmpKey.class, true);
    }
    public int compare(WritableComparable e1, WritableComparable e2) {
      EmpKey k1 = (EmpKey) e1;
      EmpKey k2 = (EmpKey) e2;
      int cmp = EmpKey.compare(k1.getSal(), k2.getSal());
      if (cmp != 0) {
        return cmp;
      }
      return -1*cmp;
    }
  }

	@Override
	public int run(String[] arg0) throws Exception {

		Configuration conf = new Configuration();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",
				",");

		Job job = Job.getInstance(conf);

		job.setJobName("Top 10 salried employees");
		job.setJarByClass(EmpDriverWithSecSort.class);
		
		job.setMapperClass(EmpMapper.class);
		job.setReducerClass(EmpReducer.class);
		job.setSortComparatorClass(EmpKeyComparator.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);


		job.setOutputKeyClass(EmpKey.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));


		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String... args) throws Exception {
		int status = ToolRunner.run(new EmpDriverWithSecSort(), args);
		System.exit(status);
	}

}
