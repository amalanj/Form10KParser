package com.csc.training.parse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ParseForm10kJob extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {

		JobConf conf = new JobConf(getConf(), ParseForm10kJob.class);
		conf.setJobName("ParseForm10kJob");

		conf.setMapOutputKeyClass(ParseForm10kMapKey.class);
		conf.setMapOutputValueClass(FloatWritable.class);

		conf.setMapperClass(ParseForm10kMapper.class);
		conf.setReducerClass(ParseForm10kReducer.class);

		conf.setInputFormat(WholeFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(FloatWritable.class);
		
		FileInputFormat.setInputPaths(conf, arg0[0]);
		FileOutputFormat.setOutputPath(conf, new Path(arg0[1]));
		
		JobClient.runJob(conf);

		return 0;

		
	}

	public static void main(String[] args) throws Exception {

		System.out.println("Start of job");
		System.exit(ToolRunner.run(new Configuration(), new ParseForm10kJob(), args));
	}
}
