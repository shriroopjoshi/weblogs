package weblogs.analysis.prep.kpi;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LogPreprocessor {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 0) {
			System.err.println("Usage: LogPreprocessor <input> <output>");
			System.exit(2);
		}
		Job job = new Job(conf, "Log Preprocessor");
		job.setJobName("Log Preprocessor");
		job.setJarByClass(LogPreprocessor.class);
		job.setMapperClass(LogPrepMapper.class);
		job.setReducerClass(LogPrepReducer.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LogOutputWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		// MultipleOutputs.addNamedOutput(job, "text", TextOutputFormat.class,
		// Text.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "ParsedRecords", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "BadRecords", TextOutputFormat.class, NullWritable.class, Text.class);

		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path("hdfs:/user/priyank/output05"), true);
		FileOutputFormat.setOutputPath(job, new Path("hdfs:/logs/output"));
		FileInputFormat.addInputPath(job, new Path("hdfs:/logs/input/sample_logs"));
		try {
			FileOutputFormat.setOutputPath(job, new Path("hdfs:/logs/output"));
		} catch (Exception e) {
			// Runtime.getRuntime().exec("hadoop dfs -rmr
			// hdfs:/user/priyank/output05");

		}
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
