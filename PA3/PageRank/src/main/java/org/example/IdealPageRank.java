package org.example;

import com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IdealPageRank {
    public void calculate(JavaPairRDD<String, Iterable<String>> links, Map<Integer, String> titlesMap, String outputPath, JavaSparkContext sc) {
        JavaPairRDD<String, Double> ranks = links.mapValues(rs -> 1.0);

        // Run the Idealized PageRank algorithm for 25 iterations
        for (int current = 0; current < 25; current++) {
            JavaPairRDD<String, Double> contribs = links.join(ranks).values()
                    .flatMapToPair(s -> {
                        int urlCount = Iterables.size(s._1());
                        List<Tuple2<String, Double>> results = new ArrayList<>();
                        for (String neighbor : s._1()) {
                            results.add(new Tuple2<>(neighbor, s._2() / urlCount));
                        }
                        return results.iterator();
                    });

            // Calculate ranks by applying damping factor
            ranks = contribs.reduceByKey((a, b) -> a + b).mapValues(sum -> 0.15 + sum * 0.85);
        }

        // Collect and sort by rank descending order
        List<Tuple2<String, Double>> idealPageRank = new ArrayList<>(ranks.collect());
        idealPageRank.sort((a, b) -> Double.compare(b._2(), a._2()));

        // Prepare output
        List<String> idealOutput = new ArrayList<>();
        for (Tuple2<String, Double> rank : idealPageRank) {
            int pageId = Integer.parseInt(rank._1());
            idealOutput.add(titlesMap.get(pageId) + " has rank: " + rank._2());
        }

        // Save output
        JavaRDD<String> idealOutputRDD = sc.parallelize(idealOutput);
        idealOutputRDD.saveAsTextFile(outputPath);
    }
}
