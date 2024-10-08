package ProfileA;

import java.util.ArrayList;
import java.util.List;

import java.io.IOException;

import javax.naming.Context;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class TFMapReduce {
    public static class TFMapper extends Mapper<Text, Text, Text, Text> {
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
}
