package ProfileA;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;

import ProfileA.UnigramFrequencyMapReduce.UnigramFrequencyMapper;
import ProfileA.UnigramFrequencyMapReduce.UnigramFrequencyReducer;

import ProfileA.TFMapReduce.TFMapper;
import ProfileA.TFMapReduce.TFReducer;

import ProfileA.TFIDFMapReduce.TFIDFMapper;
import ProfileA.TFIDFMapReduce.TFIDFReducer;

public class ProfileA extends Configured implements Tool {	
	public static int runJob(Configuration conf, String inputDir, String outputDir) throws Exception {
		// === Job 1: Unigram Frequency Calculation ===
		Job job1 = Job.getInstance(conf, "Unigram Frequency Job");
		job1.setJarByClass(UnigramFrequencyMapReduce.class);
		job1.setMapperClass(UnigramFrequencyMapper.class);
		job1.setReducerClass(UnigramFrequencyReducer.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path(inputDir));
		Path job1OutputPath = new Path("job1_output");
		FileOutputFormat.setOutputPath(job1, job1OutputPath);
		job1.waitForCompletion(true);
		long totalDocuments = job1.getCounters().findCounter("DocumentCounter", "TotalDocuments").getValue();
		conf.setLong("TotalDocuments", totalDocuments);

		// === Job 2: TF Calculation ===
		Job job2 = Job.getInstance(conf, "Calculate TF Job");
		job2.setInputFormatClass(FrequencyToKeyValueInputFormat.class);
		job2.setJarByClass(TFMapReduce.class);
		job2.setMapperClass(TFMapper.class);
		job2.setReducerClass(TFReducer.class);
		job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, job1OutputPath);
		Path job2OutputPath = new Path("job2_output");
		FileOutputFormat.setOutputPath(job2, job2OutputPath);
		job2.waitForCompletion(true);

		// === Job 3: TF Calculation ===
		Job job3 = Job.getInstance(conf, "TF-IDF Calculation Job");
		job3.setInputFormatClass(FrequencyToKeyValueInputFormat.class);
        job3.setJarByClass(TFIDFMapReduce.class);
        job3.setMapperClass(TFIDFMapper.class);
        job3.setReducerClass(TFIDFReducer.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, job2OutputPath);
        FileOutputFormat.setOutputPath(job3, new Path(outputDir));

		return job3.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ProfileA(), args);
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