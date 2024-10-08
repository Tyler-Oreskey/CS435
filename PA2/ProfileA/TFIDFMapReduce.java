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
			// Unpack the input <docID, (unigram TFvalue)>
			String[] parts = unigramFrequency.toString().replace("(", "").replace(")", "").split(", ");
			String unigram = parts[0];
			double frequency = Double.parseDouble(parts[1]);
            
            // output as <unigram, (docID TFvalue)>
			context.write(new Text(unigram), new Text("(" + docID.toString() + ", " + frequency + ")"));
        }
    }

	public static class TFIDFReducer extends Reducer<Text, Text, Text, Text> {
        private long totalDocuments; // N

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            totalDocuments = context.getConfiguration().getLong("TotalDocuments", 0);
        }

        @Override
        public void reduce(Text unigram, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Double> unigramCounts = new HashMap<>(); // To count occurrences of each unigram
            int documentCount = 0; // To track how many documents contain the unigram

            for (Text value : values) {
                String[] parts = value.toString().replaceAll("[()]", "").split(", ");
                if (parts.length == 2) {
                    String docID = parts[0]; // Extract docID
                    double tfValue = Double.parseDouble(parts[1]); // extract TF value

                    // Store the TF value per document
                    unigramCounts.put(docID, tfValue);
                    documentCount++;
                }
            }

            for (Map.Entry<String, Double> entry : unigramCounts.entrySet()) {
                String docID = entry.getKey();
                double tfValue = entry.getValue();
                double idfValue = 0;

                // Calculate IDF
                if (documentCount > 0) {
                    idfValue = Math.log10((double) totalDocuments / documentCount);
                }

                // Calculate TF-IDF
                double tfidfValue = tfValue * idfValue;

                // output as <docID, (unigram TF-IDF value)>
                context.write(new Text(docID), new Text("(" + unigram.toString() + ", " + tfidfValue + ")"));
            }
        }
    }
}
