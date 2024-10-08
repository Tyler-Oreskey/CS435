package ProfileA;

import java.util.HashMap;
import java.util.Map;

import java.io.IOException;

import javax.naming.Context;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class TFIDFMapReduce {
    public static class TFIDFMapper extends Mapper<Text, Text, Text, Text> {
        public void map(Text docID, Text unigramFrequency, Context context) throws IOException, InterruptedException {
			// Receives <docID, (unigram frequency) from InputFormatter
			// Needs to output <unigram, (docID, TF value)>

			// Unpack the unigram frequency input
			String[] parts = unigramFrequency.toString().replace("(", "").replace(")", "").split(", ");
			String unigram = parts[0]; // Extract unigram
			double frequency = Double.parseDouble(parts[1]); // Extract frequency

			// Create the output in the form <unigram, (docID, TF value)>
			context.write(new Text(unigram), new Text("(" + docID.toString() + ", " + frequency + ")"));
        }
    }

	public static class TFIDFReducer extends Reducer<Text, Text, Text, Text> {
        private long totalDocuments; // N

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Fetch the total number of documents counter value from the previous map job
            totalDocuments = context.getConfiguration().getLong("TotalDocuments", 0);
        }

        @Override
        public void reduce(Text unigram, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Double> unigramCounts = new HashMap<>(); // To count occurrences of each unigram
            int documentCount = 0; // To track how many documents contain each unigram

            // Iterate through all the input values and find ni (number of documents containing each unigram)
            for (Text value : values) {
                String valueStr = value.toString();
                String[] parts = valueStr.replaceAll("[()]", "").split(", ");
                if (parts.length == 2) {
                    String docID = parts[0]; // Extract docID
                    double tfValue = Double.parseDouble(parts[1]); // extract TF value

                    // Count the occurrence of this unigram
                    unigramCounts.put(docID, tfValue);
                    documentCount++; // Increment document count for this unigram
                }
            }

            // Calculate IDF and TF-IDF for each unigram
            for (Map.Entry<String, Double> entry : unigramCounts.entrySet()) {
                String docID = entry.getKey();
                double tfValue = entry.getValue();

                double idfValue = 0; // Initialize IDF value

                // Prevent division by zero
                if (documentCount > 0) {
					// Calculate IDF
                    idfValue = Math.log10((double) totalDocuments / documentCount);
                }

                // Calculate TF-IDF
                double tfidfValue = tfValue * idfValue;

                // Output will be in the form: <docID, (unigram TF-IDF value)>
                context.write(new Text(docID), new Text("(" + unigram.toString() + ", " + tfidfValue + ")"));
            }
        }
    }
}
