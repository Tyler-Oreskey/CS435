package ngram;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.UUID;

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
import org.apache.log4j.Logger;

public class NgramMapReduce extends Configured implements Tool {
	private static int CurrentVolume = 1;

	public static enum Profiles {
		A1('a', 1),
		B1('b', 1),
		A2('a', 2),
		B2('b', 2);

		private final char profileChar;
		private final int ngramNum;

		private Profiles(char c, int n) {
			this.profileChar = c;
			this.ngramNum = n;
		}

		public boolean equals(Profiles p) {
			return this.profileChar == p.profileChar && this.ngramNum == p.ngramNum;
		}
	}

	public static class TokenizerMapper extends Mapper<Object, BytesWritable, Text, VolumeWriteable> {

		private VolumeWriteable volume = new VolumeWriteable(new MapWritable(), new IntWritable(0));

		public void map(Object key, BytesWritable bWriteable, Context context)
				throws IOException, InterruptedException {
			Profiles profile = context.getConfiguration().getEnum("profile", Profiles.A1); // get profile

			// Generate a UUID to uniquely identify the book
			String bookUUID = UUID.randomUUID().toString();

			// code to get a book
			String rawText = new String(bWriteable.getBytes());
			Book book = new Book(rawText, profile.ngramNum);
			StringTokenizer itr = new StringTokenizer(book.getBookBody());

			// book info
			String bookAuthor = book.getBookAuthor();
			String bookYear = book.getBookYear();

			// Insert the book UUID and count into volume
			volume.insertMapValue(new Text(bookUUID), new IntWritable(1));

			// #TODO#: Define any helper variables you need before looping through tokens
			while (itr.hasMoreTokens()) {
				String currentWord = itr.nextToken().trim();

				if (!currentWord.isEmpty()) {
					switch (profile) {
						case A1:
							Text unigramYearKey = new Text(currentWord + "\t" + bookYear);
							context.write(unigramYearKey, volume);
							break;
						default:
							break;
					}
				}
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, VolumeWriteable, Text, VolumeWriteable> {
		// #TODO#: Initialize any necessary class variables

		public void reduce(Text key, Iterable<VolumeWriteable> values, Context context)
				throws IOException, InterruptedException {
			// #TODO#: Implement the reduce method
			// hint: Aggregate the counts and volume information
		}
	}

	public static int runJob(Configuration conf, String inputDir, String outputDir) throws Exception {
		// function to run job
		Job job = Job.getInstance(conf, "ngram");

		// specify classes for Map Reduce tasks
		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setJarByClass(NgramMapReduce.class);

		job.setMapperClass(TokenizerMapper.class);
		// job.setCombinerClass(SPECIFYCLASS.class);
		job.setReducerClass(IntSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VolumeWriteable.class);

		FileInputFormat.addInputPath(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		// ToolRunner allows for command line configuration parameters - suitable for
		// shifting between local job and yarn
		// example command: hadoop jar <path_to_jar.jar> <main_class> -D param=value
		// <input_path> <output_path>
		// We use -D mapreduce.framework.name=<value> where <value>=local means the job
		// is run locally and <value>=yarn means using YARN
		int res = ToolRunner.run(new Configuration(), new NgramMapReduce(), args);
		System.exit(res); // res will be 0 if all tasks are executed succesfully and 1 otherwise
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Profiles profiles[] = { Profiles.A1, Profiles.A2, Profiles.B1, Profiles.B2 };
		for (Profiles p : profiles) {
			conf.setEnum("profile", p);
			System.out.println("For profile: " + p.toString());
			if (runJob(conf, args[0], args[1] + p.toString()) != 0)
				return 1; // error
		}
		return 0; // success
	}
}