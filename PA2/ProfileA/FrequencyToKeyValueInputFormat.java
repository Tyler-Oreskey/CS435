package ProfileA;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class FrequencyToKeyValueInputFormat extends FileInputFormat<LongWritable, Text> {

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return true;
	}

    @Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		FrequencyToKeyValueRecordReader reader = new FrequencyToKeyValueRecordReader();
		reader.initialize(split, context);
		return reader;
	}
}