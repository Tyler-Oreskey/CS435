package org.example;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

import com.google.common.collect.Iterables;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

public final class PageRank {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: JavaPageRank <links_file> <titles_file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("PageRank")
                .master("local[5]")
                .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD(); // read links
        JavaRDD<String> titles = spark.read().textFile(args[1]).javaRDD(); // read titles

        // create a map of page IDs to titles
        Map<Integer, String> titlesMap = titles.zipWithIndex()
                .mapToPair(t -> new Tuple2<>((int) (t._2() + 1), t._1()))
                .collectAsMap();

        JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(s -> {
            String[] parts = s.split(": ");
            return new Tuple2<>(parts[0], parts[1]);
        }).distinct().groupByKey().cache();

        // Initialize each page's rank to 1.0
        JavaPairRDD<String, Double> ranks = links.mapValues(rs -> 1.0);

        // Calculate total pages once
        long totalPages = links.count();
        double beta = 0.85;
        int iterations = 10;

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
            ranks = contribs.reduceByKey((a, b) -> a + b)
                    .mapValues(sum -> 0.15 + sum * beta);
        }

        // Collect and sort by rank (Ideal PageRank)
        List<Tuple2<String, Double>> idealPageRank = new ArrayList<>(ranks.collect());
        idealPageRank.sort((a, b) -> Double.compare(b._2(), a._2())); // Descending order
        // Display the top results with titles (Ideal PageRank)
        System.out.println("Ideal PageRank Results:");
        for (Tuple2<String, Double> rank : idealPageRank) {
            int pageId = Integer.parseInt(rank._1());
            System.out.println(titlesMap.get(pageId) + " has rank: " + rank._2());
        }


        // Reset ranks for Taxation
        ranks = links.mapValues(rs -> 1.0);

        // Run the Taxation PageRank algorithm for 25 iterations
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

            // Calculate ranks by applying taxation
            ranks = contribs.reduceByKey((a, b) -> a + b)
                    .mapValues(sum -> (1 - beta) / totalPages + beta * sum);
        }

        // Collect and sort by rank (PageRank with Taxation)
        List<Tuple2<String, Double>> taxationPageRank = new ArrayList<>(ranks.collect());
        taxationPageRank.sort((a, b) -> Double.compare(b._2(), a._2())); // Descending order

        // Display the top results with titles (Taxation PageRank)
        System.out.println("PageRank with Taxation Results:");
        for (Tuple2<String, Double> rank : taxationPageRank) {
            int pageId = Integer.parseInt(rank._1());
            System.out.println(titlesMap.get(pageId) + " has rank: " + rank._2());
        }

        spark.stop();
    }
}
