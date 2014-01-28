package com.csc.training.parse;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ParseForm10kReducer extends MapReduceBase implements Reducer<ParseForm10kMapKey, IntWritable, Text, IntWritable> {
	
	@Override
	public void reduce(ParseForm10kMapKey key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			
		Text reduceKey = new Text();
		IntWritable reduceValue = new IntWritable();
		int sum = 0;
		
		while(values.hasNext()){
			sum+=values.next().get();
		}
		reduceKey.set(key.getCompanyName()+","+key.getYear());
		reduceValue.set(sum);
				
		output.collect(reduceKey, reduceValue);
	}

}
