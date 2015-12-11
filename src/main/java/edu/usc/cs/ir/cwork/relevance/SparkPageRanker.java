package edu.usc.cs.ir.cwork.relevance;

import org.apache.commons.io.IOUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import scala.Tuple2;

import com.google.common.collect.Iterables;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.regex.Pattern;

/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.graphx.lib.PageRank
 */
public final class SparkPageRanker implements Closeable {

    /**
     * Pattern for breaking a line into edges
     */
    private static final Pattern SPACES = Pattern.compile("\\s+");

    /**
     * Adds two double values
     */
    private static Function2<Double, Double, Double> sumFunc = (a, b) -> a + b;


    @Option(name = "-edges", required = true, usage = "Path to file having graph's edges")
    private File edgesFile;

    @Option(name = "-out", required = true, usage = "Path to Output file for storing page ranks")
    private File outFile;

    @Option(name = "-n", required = true, usage = "Number of Iteration")
    private int numIterations = 5;

    @Option(name = "-t",  usage = "Number of Threads")
    private int numThreads = 2;


    private JavaSparkContext spCtx;


    public void init(){
        SparkConf sparkConf = new SparkConf()
                .setMaster(String.format("local[%d]", numThreads))
                .setAppName("Page-Ranker");
        spCtx = new JavaSparkContext(sparkConf);
    }


    public JavaPairRDD<String, Double> run(){

        // Loads in input file. It should be in format of:
        //     URL         neighbor URL
        //     URL         neighbor URL
        //     URL         neighbor URL
        //     ...
        JavaRDD<String> lines = spCtx.textFile(edgesFile.getPath(), 1);

        // Loads all URLs from input file and initialize their neighbors.
        JavaPairRDD<String, Iterable<String>> links = lines
                .mapToPair(s -> {
                    String[] parts = SPACES.split(s);
                    return new Tuple2<>(parts[0], parts[1]);
                }).distinct()
                .groupByKey()
                .cache();

        // Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
        JavaPairRDD<String, Double> ranks = links.mapValues( rs -> 1.0);

        // Calculates and updates URL ranks continuously using PageRank algorithm.
        for (int i = 0; i < numIterations; i++) {
            // Calculates URL contributions to the rank of other URLs.
            JavaPairRDD<String, Double> contribs = links.join(ranks).values()
                    .flatMapToPair((PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>) s -> {
                        int urlCount = Iterables.size(s._1);
                        List<Tuple2<String, Double>> results = new ArrayList<>();
                        for (String n : s._1) {
                            results.add(new Tuple2<>(n, s._2() / urlCount));
                        }
                        return results;
                    });

            // Re-calculates URL ranks based on neighbor contributions.
            ranks = contribs.reduceByKey(sumFunc)
                    .mapValues(sum -> 0.15 + sum * 0.85);
        }

        return ranks;
    }


    @Override
    public void close() throws IOException {
        if (spCtx != null) {
            spCtx.stop();
        }
        IOUtils.closeQuietly(spCtx);
    }



    public static void main(String[] args) throws IOException {
        //args = "-edges locations.txt -n 5 -out pr-loc.txt".split(" ");
        SparkPageRanker ranker = new SparkPageRanker();
        CmdLineParser parser = new CmdLineParser(ranker);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getLocalizedMessage());
            parser.printUsage(System.err);
            return;
        }

        ranker.init();
        JavaPairRDD<String, Double> ranks = ranker.run();

        // Collects all URL ranks and dump them to console.
        Iterator<Tuple2<String, Double>> output = ranks.toLocalIterator();

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(ranker.outFile))) {
            while(output.hasNext()) {
                Tuple2<?, ?> tuple = output.next();
                System.out.println();
                writer.write(tuple._1() + "\t" + tuple._2() + '\n');
            }
        }
    }

}