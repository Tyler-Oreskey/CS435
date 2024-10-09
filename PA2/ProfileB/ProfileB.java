package ProfileB;

import java.net.URI;
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

public class ProfileB extends Configured implements Tool {	
	public static int runJob(Configuration conf, String tfidfPath, String articleInputPath, String outputPath) throws Exception {
		Job job = Job.getInstance(conf, "Document Summarization Job");
		job.setJarByClass(ProfileB.class);

		// Specify mapper and reducer classes
		job.setMapperClass(ProfileBMapper.class);   
		job.setReducerClass(ProfileBReducer.class);

        // Set output key and value classes for mapper and reducer
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(articleInputPath));  // Articles to be summarized
        FileOutputFormat.setOutputPath(job, new Path(outputPath));      // Output path for the summaries

		// Add TF-IDF file to the distributed cache
		job.addCacheFile(new URI(tfidfPath));

        // Run the job and return the completion status
        return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ProfileB(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        if (args.length != 3) {
            System.err.println("Usage: ProfileB <input path> <input path> <output path>");
            System.exit(1);
        }
        return runJob(conf, args[0], args[1], args[2]);
	}
}