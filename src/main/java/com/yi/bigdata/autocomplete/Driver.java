package com.yi.bigdata.autocomplete;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;


public class Driver {
	private static Logger logger = Logger.getLogger(Driver.class);

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		//job1
		Configuration configuration1 = new Configuration();

		//customize separator
		configuration1.set("textinputformat.record.delimiter", ".");
		configuration1.set("noGram", args[2]);

		Job job1 = Job.getInstance(configuration1);
		job1.setJobName("NGram");
		job1.setJarByClass(Driver.class);

		job1.setMapperClass(NGramLibraryBuilder.NGramMapper.class);
		job1.setReducerClass(NGramLibraryBuilder.NGramReducer.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		TextInputFormat.setInputPaths(job1, new Path(args[0]));
		TextOutputFormat.setOutputPath(job1, new Path(args[1]));

		logger.info("Driver: main(): Job1 has started");
		job1.waitForCompletion(true);
		
		//2nd job
		Configuration configuration2 = new Configuration();
		configuration2.set("threshold", args[3]);
		configuration2.set("topN", args[4]);

		Job job2 = Job.getInstance(configuration2);
		job2.setJobName("Model");
		job2.setJarByClass(Driver.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(NullWritable.class);

		job2.setMapperClass(LanguageModel.Map.class);
		job2.setReducerClass(LanguageModel.Reduce.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		TextInputFormat.setInputPaths(job2, new Path(args[1]));
		TextOutputFormat.setOutputPath(job2, new Path(args[1] + "Job2"));

		logger.info("Driver: main(): Job2 has started");
		job2.waitForCompletion(true);

	}

}
