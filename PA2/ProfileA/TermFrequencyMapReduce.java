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

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Writable;

public class TermFrequencyMapReduce extends Configured implements Tool {

	public static class TermFrequencyMapper extends Mapper<Object, Text, Text, IntWritable> {

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
					context.write(new Text(articleID + " " + currentWord), new IntWritable(1));
				}
			}
		}
	}

	public static class TermFrequencyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;

			for (IntWritable val : values) {
				sum += val.get();
			}

			result.set(sum);
			context.write(key, result);
		}
	}

	public static int runJob(Configuration conf, String inputDir, String outputDir) throws Exception {
		// function to run job1
		Job job1 = Job.getInstance(conf, "Term Frequency Job");
		job1.setJarByClass(TermFrequencyMapReduce.class);
		job1.setMapperClass(TermFrequencyMapper.class);
		job1.setReducerClass(TermFrequencyReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job1, new Path(inputDir));
		FileOutputFormat.setOutputPath(job1, new Path(outputDir));

		return job1.waitForCompletion(true) ? 0 : 1;
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