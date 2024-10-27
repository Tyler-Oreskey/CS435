package org.example;

import java.util.Map;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaSparkContext;

public final class PageRank {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: JavaPageRank <links_file> <titles_file> <ideal_output> <taxation_output>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("PageRank")
                .master("yarn")
                .getOrCreate();

        // Get the JavaSparkContext from SparkSession
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

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

        // Execute Ideal PageRank
        IdealPageRank idealPageRank = new IdealPageRank();
        idealPageRank.calculate(links, titlesMap, args[2], sc);

        // Execute Taxation PageRank
        TaxationPageRank taxationPageRank = new TaxationPageRank();
        taxationPageRank.calculate(links, titlesMap, args[3], sc);

        spark.stop();
        sc.stop();
    }
}
