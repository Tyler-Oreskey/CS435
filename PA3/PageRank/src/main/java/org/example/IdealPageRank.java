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
        double beta = 0.85;
        int iterations = 25;

        // Run the Idealized PageRank algorithm for 25 iterations
        for (int current = 0; current < iterations; current++) {
            JavaPairRDD<String, Double> contribs = links.join(ranks).values()
                    .flatMapToPair(s -> {
                        int urlCount = Iterables.size(s._1());
                        List<Tuple2<String, Double>> results = new ArrayList<>();
                        for (String neighbor : s._1()) {
                            results.add(new Tuple2<>(neighbor, s._2() / urlCount));
                        }
                        return results.iterator();
                    });

            // Recalculate ranks by applying damping factor
            ranks = contribs.reduceByKey((a, b) -> a + b).mapValues(sum -> 0.15 + sum * beta);
        }

        // Collect and sort by rank (Ideal PageRank)
        List<Tuple2<String, Double>> idealPageRank = new ArrayList<>(ranks.collect());
        idealPageRank.sort((a, b) -> Double.compare(b._2(), a._2())); // Descending order

        List<String> idealOutput = new ArrayList<>();
        // Prepare output for Ideal PageRank
        for (Tuple2<String, Double> rank : idealPageRank) {
            int pageId = Integer.parseInt(rank._1());
            idealOutput.add(titlesMap.get(pageId) + " has rank: " + rank._2());
        }

        // Save Ideal PageRank results to output file in HDFS
        JavaRDD<String> idealOutputRDD = sc.parallelize(idealOutput);
        idealOutputRDD.saveAsTextFile(outputPath);
    }
}
