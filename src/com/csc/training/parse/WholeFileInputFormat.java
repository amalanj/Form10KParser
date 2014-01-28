package com.csc.training.parse;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * @author amalan
 * Custom file input format to read complete file as filesplits
 *
 */
public class WholeFileInputFormat extends FileInputFormat<NullWritable, ParseForm10kCustomRecord> {
	@Override
	protected boolean isSplitable(FileSystem fs, Path filename) {
		return false;
	}

	@Override
	public RecordReader<NullWritable, ParseForm10kCustomRecord> getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException {

		return new WholeFileRecordReader((FileSplit) split, job);
	}

}