package ProfileB;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import javax.naming.Context;

public class ProfileBMapper extends Mapper<LongWritable, Text, Text, Tuple> {

    // HashMap to store TF-IDF data from distributed cache
    private Map<String, Double> tfidfMap = new HashMap<>();

    // Setup method to load TF-IDF data from the distributed cache
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            BufferedReader reader = new BufferedReader(new FileReader(new File("./" + new Path(cacheFiles[0].getPath()).getName())));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\s+", 2);
                if (parts.length == 2) {
                    String docID = parts[0].trim();
                    String[] tupleParts = parts[1].replaceAll("[()]", "").split(", ");
                    if (tupleParts.length == 2) {
                        String unigram = tupleParts[0].trim();
                        double tfidf = Double.parseDouble(tupleParts[1].trim());
                        String key = docID + ":" + unigram;
                        tfidfMap.put(key, tfidf);
                    }
                }
            }
            reader.close();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Parse input text as an article
        Article article = new Article(value.toString());
        Text docID = new Text(article.getDocID());
        String[] sentences = article.getSentences();

        if (docID.toString().equals("Unknown")) {
            return;
        }

        // Process each sentence to calculate the sentence TF-IDF score
        for (String sentence : sentences) {
            double sentenceScore = calculateSentenceTFIDF(article.getDocID(), sentence);
            context.write(docID, new Tuple(sentence, sentenceScore));
        }
    }

    private double calculateSentenceTFIDF(String docID, String sentence) {
        String[] words = sentence.split("\\s+");
        double score = 0.0;
        int wordCount = 0;

        for (String word : words) {
            String key = docID + ":" + word.toLowerCase();
            if (tfidfMap.containsKey(key)) {
                score += tfidfMap.get(key);
                wordCount++;
            }
        }

        // Return average score or 0 if no words were found in TF-IDF map
        return (wordCount > 0) ? score / wordCount : 0.0;
    }
}
