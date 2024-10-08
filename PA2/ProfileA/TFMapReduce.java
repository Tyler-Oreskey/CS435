package ProfileA;

import java.util.ArrayList;
import java.util.List;

import java.io.IOException;

import javax.naming.Context;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class TFMapReduce {
    public static class TFMapper extends Mapper<Text, Tuple, Text, Tuple> {
		public void map(Text docID, Tuple value, Context context) throws IOException, InterruptedException {
			// write output as <docID, (unigram frequency)>
			context.write(docID, value);
		}
	}

	public static class TFReducer extends Reducer<Text, Tuple, Text, Tuple> {
		public void reduce(Text docID, Iterable<Tuple> values, Context context) throws IOException, InterruptedException {
			double maxFrequency = 0;
			List<Tuple> unigrams = new ArrayList<>();
	
			for (Tuple value : values) {
				String unigram = value.getFirst(); // unigram
				double tfValue = value.getSecond(); // TFvalue

				// update maxFrequency and store unigram info
				maxFrequency = Math.max(maxFrequency, tfValue);
				unigrams.add(new Tuple(unigram, tfValue));
			}
	
			for (Tuple unigramInfo : unigrams) {
				String unigram = unigramInfo.getFirst();
				double tfValue = unigramInfo.getSecond();
	
				// calculate TF
				double tf = 0.5 + 0.5 * (tfValue / maxFrequency);

				// write output as <docID, (unigram TFvalue)>
				context.write(docID, new Tuple(unigram, tf));
			}
		}
	}
}
