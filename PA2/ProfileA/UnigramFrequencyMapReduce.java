package ProfileA;

import java.io.IOException;
import java.util.StringTokenizer;

import javax.naming.Context;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class UnigramFrequencyMapReduce {
    public static class UnigramFrequencyMapper extends Mapper<Object, Text, Text, IntWritable> {
        private long documentCount = 0;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String rawText = value.toString();
			Article article = new Article(rawText);
			String docID = article.getDocID();

			if (docID.equals("Unknown")) {
				return;
			}

            // Increment the document count for valid articles
            documentCount++;

			StringTokenizer itr = new StringTokenizer(article.getBody());
			while (itr.hasMoreTokens()) {
				String currentWord = itr.nextToken().trim();

				if (!currentWord.isEmpty()) {
					context.write(new Text(docID + ":" + currentWord), new IntWritable(1));
				}
			}
		}

        @Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.getCounter("DocumentCounter", "TotalDocuments").increment(documentCount);
		}
	}

	public static class UnigramFrequencyReducer extends Reducer<Text, IntWritable, Text, Tuple> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;

			for (IntWritable value : values) {
				sum += value.get();
			}

			String[] parts = key.toString().split(":");
            String docID = parts[0];
            String unigram = parts[1];

			// output as <docID, (unigram, frequency)>
			context.write(new Text(docID), new Tuple(unigram, sum)); 
		}
	}
}
