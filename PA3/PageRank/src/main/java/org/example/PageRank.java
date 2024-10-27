package org.example;

import java.util.Map;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaSparkContext;

public final class PageRank {
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: JavaPageRank <links_file> <titles_file> <type> <output_dir>");
            System.exit(1);
        }

        String type = args[2].toLowerCase();
        if (!type.equals("ideal") && !type.equals("taxation")) {
            System.err.println("Error: Type must be either 'ideal' or 'taxation'");
            System.exit(1);
        }

        SparkSession spark = SparkSession.builder().appName("PageRank").master("yarn").getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        // Read links and titles
        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
        JavaRDD<String> titles = spark.read().textFile(args[1]).javaRDD();

        // create a map of page IDs to titles
        Map<Integer, String> titlesMap = titles.zipWithIndex()
                .mapToPair(t -> new Tuple2<>((int) (t._2() + 1), t._1()))
                .collectAsMap();

        // Convert each line into a key-value pair: source page as key
        // and comma-separated target pages as value for grouping.
        JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(s -> {
            String[] parts = s.split(": ");
            return new Tuple2<>(parts[0], parts[1]);
        }).distinct().groupByKey().cache();

        if (type.equals("ideal")) {
            IdealPageRank idealPageRank = new IdealPageRank();
            idealPageRank.calculate(links, titlesMap, args[3], sc);
        } else {
            TaxationPageRank taxationPageRank = new TaxationPageRank();
            taxationPageRank.calculate(links, titlesMap, args[3], sc);
        }

        spark.stop();
        sc.stop();
    }
}
