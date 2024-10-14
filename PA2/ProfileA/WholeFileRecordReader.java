package ProfileA;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

public class WholeFileRecordReader extends RecordReader<Text, Tuple> {
    private LineRecordReader lineReader = new LineRecordReader();
    private Text key = new Text();
    private Tuple value;

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
                String docID = parts[0].trim();
                String[] tupleParts = parts[1].replaceAll("[()]", "").split(", "); // Split the (unigram, frequency) part

                if (tupleParts.length == 2) {
                    String unigram = tupleParts[0].trim();
                    double frequency = Double.parseDouble(tupleParts[1].trim());

                    key.set(docID);
                    value = new Tuple(unigram, frequency);
                    return true;
                }
            }
        }
        return false; // No more records to process
    }
    

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Tuple getCurrentValue() throws IOException, InterruptedException {
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
