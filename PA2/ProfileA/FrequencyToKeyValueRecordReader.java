package ProfileA;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

public class FrequencyToKeyValueRecordReader extends RecordReader<LongWritable, Text> {
    private LineRecordReader lineReader = new LineRecordReader();
    private LongWritable key = new LongWritable();
    private Text value = new Text();

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        lineReader.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        // Read the next line
        if (lineReader.nextKeyValue()) {
            // Get the current line value
            String lineStr = lineReader.getCurrentValue().toString().trim();
            String[] parts = lineStr.split("\\s+", 2);
    
            if (parts.length == 2) {
                String articleIdStr = parts[0].trim();
                String unigramStr = parts[1].trim(); // This will include (002, 1)

                key.set(Long.parseLong(articleIdStr));
                value.set(unigramStr);
                return true;
            }
        }
        return false; // No more records to process
    }
    

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException {
        return lineReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        lineReader.close();
    }
}
