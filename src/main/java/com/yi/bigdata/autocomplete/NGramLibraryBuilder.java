package com.yi.bigdata.autocomplete;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;


public class NGramLibraryBuilder {

	public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private static Logger logger = Logger.getLogger(NGramMapper.class);
		int noGram;
		@Override
		public void setup(Context context) {
			//how to get n-gram from command line?
			Configuration configuration = context.getConfiguration();
			noGram = configuration.getInt("noGram", 5);
		}

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			logger.info("NGramLibraryBuilder: NgramMapper: ngram mapper started");
			String line = value.toString().trim().toLowerCase();
			System.out.println("#NGramLibraryBuilder: NgramMapper: read line:" + line);
			//remove useless elements
			line = line.replaceAll("[^a-z]", " ");

			//separate word by space
			String[] words = line.split("\\s+");
			System.out.println(words.length);

			//build n-gram based on array of words
			if (words.length < 2) {
				return;
			}
			for (int i = 0; i < words.length; i++) {
				StringBuilder sb = new StringBuilder();
				sb.append(words[i]);
				for (int j = 1; j < noGram && i + j < words.length; j++) {
					sb.append(" ");
					sb.append(words[i + j]);
					String str = sb.toString().trim();
					logger.debug("NGramLibraryBuilder: NgramMapper: ngram result string: " + str);
					System.out.println("NGramLibraryBuilder: NgramMapper: ngram result string:" + str);
					context.write(new Text(str), new IntWritable(1));
				}
			}
		}
	}

	public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			//sum up the total count for each n-gram
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

}