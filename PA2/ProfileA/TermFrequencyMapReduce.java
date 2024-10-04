package ProfileA;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.naming.Context;

import java.util.UUID;
import java.util.Set;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;


import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Writable;

public class TermFrequencyMapReduce extends Configured implements Tool {

	public static class TermFrequencyMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text wordWithID = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String rawText = new String(value.toString());

			Article article = new Article(rawText);
			String articleID = article.getArticleID();
			String bodyText = article.getArticleBody();

			if (articleID.equals("Unknown")) {
				return;
			}

			StringTokenizer itr = new StringTokenizer(article.getArticleBody());
			while (itr.hasMoreTokens()) {
				String currentWord = itr.nextToken().trim();

				if (!currentWord.isEmpty()) {
					wordWithID.set(articleID + " " + currentWord);
					context.write(wordWithID, one);
				}
			}
		}
	}

	public static class TermFrequencyReducer extends Reducer<Text, IntWritable, Text, Text> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;

			for (IntWritable value : values) {
				sum += value.get();
			}

			String[] keyParts = key.toString().split(" ");
			String articleID = keyParts[0];
			String unigram = keyParts[1];

			context.write(new Text(articleID), new Text("(" + unigram + ", " + sum + ")")); 
		}
	}

	public static class IdentityMapper extends Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static class TFReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text articleID, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double maxFrequency = 0;
			List<String> unigrams = new ArrayList<>(); // To store unigram information
	
			// First pass: find the maximum frequency and collect unigram data
			for (Text value : values) {
				String valueStr = value.toString();
				// Remove parentheses and split by comma
				String[] parts = valueStr.replaceAll("[()]", "").split(", ");
				if (parts.length == 2) {
					String unigram = parts[0]; // unigram
					double tfValue = Double.parseDouble(parts[1]); // TFvalue
	
					// Update maxFrequency
					maxFrequency = Math.max(maxFrequency, tfValue);
					
					// Store unigram info for the next pass
					unigrams.add(unigram + "," + tfValue);
				}
			}
	
			// Second pass: calculate TF for each unigram using the stored data
			for (String unigramInfo : unigrams) {
				String[] parts = unigramInfo.split(",");
				String unigram = parts[0];
				double tfValue = Double.parseDouble(parts[1]);
	
				// Calculate TF
				double tf = 0.5 + 0.5 * (tfValue / maxFrequency);
	
				// Write output as <docID, (unigram TFvalue)>
				context.write(articleID, new Text("(" + unigram + ", " + tf + ")"));
			}
		}
	}
	

	
	public static int runJob(Configuration conf, String inputDir, String outputDir) throws Exception {
		// === Job 1: Term Frequency Calculation ===
		Job job1 = Job.getInstance(conf, "Term Frequency Job");
		job1.setJarByClass(TermFrequencyMapReduce.class);
		job1.setMapperClass(TermFrequencyMapper.class);
		job1.setReducerClass(TermFrequencyReducer.class);

		// set output types for mapper
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);

		// set output types for reducer
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		// set file I/O path for job 1
		FileInputFormat.addInputPath(job1, new Path(inputDir));
		Path job1OutputPath = new Path("job1_output");
		FileOutputFormat.setOutputPath(job1, job1OutputPath);
		job1.waitForCompletion(true);

		// === Job 2: TF Calculation ===
		Job job2 = Job.getInstance(conf, "Calculate TF Job");
		job2.setInputFormatClass(FrequencyToKeyValueInputFormat.class);

		job2.setJarByClass(TermFrequencyMapReduce.class);
		job2.setMapperClass(IdentityMapper.class);
		job2.setReducerClass(TFReducer.class);

		// set output types for mapper
		job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

		// set output types for reducer
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

		// set file I/O path for job 2
		FileInputFormat.addInputPath(job2, job1OutputPath);
		FileOutputFormat.setOutputPath(job2, new Path(outputDir));

		return job2.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TermFrequencyMapReduce(), args);
		System.exit(res); // res will be 0 if all tasks are executed succesfully and 1 otherwise
	}

	@Override
	public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        if (args.length != 2) {
            System.err.println("Usage: TermFrequencyMapReduce <input path> <output path>");
            System.exit(1);
        }
        return runJob(conf, args[0], args[1]);
	}
}