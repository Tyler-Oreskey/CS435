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
			List<String> unigrams = new ArrayList<>();
	
			for (Text value : values) {
				String[] parts = value.toString().replaceAll("[()]", "").split(", ");

				if (parts.length == 2) {
					String unigram = parts[0]; // unigram
					double tfValue = Double.parseDouble(parts[1]); // TFvalue
	
					// Update maxFrequency and store unigram info
					maxFrequency = Math.max(maxFrequency, tfValue);					
					unigrams.add(unigram + "," + tfValue);
				}
			}
	
			for (String unigramInfo : unigrams) {
				String[] parts = unigramInfo.split(",");
				String unigram = parts[0];
				double tfValue = Double.parseDouble(parts[1]);
	
				// Calculate TF and write output as <articleID, (unigram TFvalue)>
				double tf = 0.5 + 0.5 * (tfValue / maxFrequency);	
				context.write(articleID, new Text("(" + unigram + ", " + tf + ")"));
			}
		}
	}
}
