package ProfileB;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import javax.naming.Context;

public class ProfileBReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // TreeMap to keep the top 5 sentences with scores (sorted by score)
        TreeMap<Double, String> topSentences = new TreeMap<>(Collections.reverseOrder()); // Descending order by score
        final int maxSentences = 5;

        for (Text value : values) {
            // Extract sentence and score using regex
            String[] parts = value.toString().split(" \\(Score: ");
            if (parts.length == 2) {
                String sentence = parts[0].trim();
                double score = Double.parseDouble(parts[1].replace(")", "").trim());

                // Add the sentence to the TreeMap
                topSentences.put(score, sentence);
                
                // Maintain the top N sentences
                if (topSentences.size() > maxSentences) {
                    topSentences.pollLastEntry(); // Remove the lowest score
                }
            }
        }

        // Build the summary from the top sentences
        StringBuilder summary = new StringBuilder();
        for (Map.Entry<Double, String> entry : topSentences.entrySet()) {
            summary.append(entry.getValue()).append(" (Score: ").append(entry.getKey()).append(") ");
        }

        // Emit the key (document ID) and the summarized text
        context.write(key, new Text(summary.toString().trim()));
    }
}
