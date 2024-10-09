package ProfileB;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ProfileBReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder summary = new StringBuilder();
        int sentenceCount = 0;
        int maxSentences = 5;

        for (Text sentence : values) {
            if (sentenceCount < maxSentences) {
                summary.append(sentence.toString()).append(" ");
                sentenceCount++;
            } else {
                break;
            }
        }

        context.write(key, new Text(summary.toString().trim()));
    }
}
