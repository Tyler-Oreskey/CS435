package ProfileA;

import java.util.HashMap;
import java.util.Map;

import java.io.IOException;

import javax.naming.Context;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class TFIDFMapReduce {
    public static class TFIDFMapper extends Mapper<Text, Tuple, Text, Tuple> {
        public void map(Text docID, Tuple unigramFrequency, Context context) throws IOException, InterruptedException {
			// Unpack the input <docID, (unigram TFvalue)>
            String unigram = unigramFrequency.getFirst();
            double frequency = unigramFrequency.getSecond();
            
            // output as <unigram, (docID TFvalue)>
            context.write(new Text(unigram), new Tuple(docID.toString(), frequency));
        }
    }

	public static class TFIDFReducer extends Reducer<Text, Tuple, Text, Tuple> {
        private long totalDocuments; // N

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            totalDocuments = context.getConfiguration().getLong("TotalDocuments", 0);
        }

        @Override
        public void reduce(Text unigram, Iterable<Tuple> values, Context context) throws IOException, InterruptedException {
            Map<String, Double> unigramCounts = new HashMap<>(); // To count occurrences of each unigram
            int documentCount = 0; // To track how many documents contain the unigram

			for (Tuple value : values) {
				String docID = value.getFirst();
				double tfValue = value.getSecond();

                // store the TF value per document
                unigramCounts.put(docID, tfValue);
                documentCount++;
			}

            for (Map.Entry<String, Double> entry : unigramCounts.entrySet()) {
                String docID = entry.getKey();
                double tfValue = entry.getValue();
                double idfValue = 0;

                // calculate IDF
                if (documentCount > 0) {
                    idfValue = Math.log10((double) totalDocuments / documentCount);
                }

                // calculate TF-IDF
                double tfidfValue = tfValue * idfValue;

                // output as <docID, (unigram TF-IDF value)>
                context.write(new Text(docID), new Tuple(unigram.toString(), tfidfValue));
            }
        }
    }
}
