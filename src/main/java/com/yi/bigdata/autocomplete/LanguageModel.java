package com.yi.bigdata.autocomplete;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		int threshold;

		@Override
		public void setup(Context context) {
			// how to get the threshold parameter from the configuration?
			Configuration configuration = context.getConfiguration();
			threshold = configuration.getInt("threshold", 1);
		}

		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// input: this is cool\t20
			if (value != null) {
				String line = value.toString().trim();
				if (line.length() > 0){
					// words[0]: this is cool
					// words[1]: 20
					String[] wordsAndCount = line.split("\t");
					if (wordsAndCount.length == 2) {
						int count = Integer.parseInt(wordsAndCount[1]);
						String[] words = wordsAndCount[0].split("\\s+");
						if (count >= threshold) {
							//output: this is : cool=20
							StringBuilder sb = new StringBuilder();
							for(int i = 0; i < words.length-1; i++) {
								sb.append(words[i]).append(" ");
							}
							String outputKey = sb.toString().trim();
							String outputValue =
									words[words.length - 1] + "=" + count;
							context.write(new Text(outputKey), new Text(outputValue));
						}
					}
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, NullWritable> {

		// Top N following word.
		int n;
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			n = conf.getInt("topN", 5);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// input: this is -> <cool=20, awesome=13>
			// priorityQueue to rank topN n-gram
			TreeMap<Integer, List<String>> tm =
					new TreeMap<Integer, List<String>>(Collections.<Integer>reverseOrder());
			for (Text value : values) {
				String valueString = value.toString().trim();
				if (StringUtils.isNotBlank(valueString)) {
					String word = valueString.split("=")[0].trim();
					int count = Integer.parseInt(valueString.split("=")[1].trim());
					if (tm.containsKey(count)) {
						tm.get(count).add(word);
					}
					else {
						List<String> list = new ArrayList<String>();
						list.add(word);
						tm.put(count, list);
					}
				}
			}

			// write to the output
			Iterator<Integer> iterator = tm.keySet().iterator();
			for (int i = 0; i < n && iterator.hasNext(); i++) {
				Integer wordCount = iterator.next();
				List<String> followingWords = tm.get(wordCount);
				for (String curWord : followingWords) {
					context.write(new Text(key.toString() + " " + curWord + " "
							+ wordCount.toString()), NullWritable.get());
				}
			}

		}
	}
}
