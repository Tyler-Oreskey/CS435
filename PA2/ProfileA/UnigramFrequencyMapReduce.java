package ProfileA;

import java.io.IOException;
import java.util.StringTokenizer;

import javax.naming.Context;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import ProfileA.Article;

public class UnigramFrequencyMapReduce {
    public static class UnigramFrequencyMapper extends Mapper<Object, Text, Text, IntWritable> {
        private long documentCount = 0;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String rawText = value.toString();
			Article article = new Article(rawText);
			String articleID = article.getArticleID();

			if (articleID.equals("Unknown")) {
				return;
			}

            // Increment the document count for valid articles
            documentCount++;

			StringTokenizer itr = new StringTokenizer(article.getArticleBody());
			while (itr.hasMoreTokens()) {
				String currentWord = itr.nextToken().trim();

				if (!currentWord.isEmpty()) {
					context.write(new Text(articleID + " " + currentWord), new IntWritable(1));
				}
			}
		}

        @Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.getCounter("DocumentCounter", "TotalDocuments").increment(documentCount);
		}
	}

	public static class UnigramFrequencyReducer extends Reducer<Text, IntWritable, Text, Text> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;

			for (IntWritable value : values) {
				sum += value.get();
			}

			String[] keyParts = key.toString().split(" ");
			String articleID = keyParts[0];
			String unigram = keyParts[1];

			context.write(new Text(articleID), new Text("(" + unigram + ", " + sum + ")")); 
		}
	}
}
