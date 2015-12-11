package edu.usc.cs.ir.cwork;

import edu.usc.cs.ir.cwork.relevance.GraphGenerator;
import edu.usc.cs.ir.cwork.relevance.SparkPageRanker;
import edu.usc.cs.ir.cwork.solr.Phase2Indexer;
import edu.usc.cs.ir.cwork.solr.SolrIndexer;
import edu.usc.cs.ir.cwork.solr.SolrPageRankUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.lang.reflect.Method;
import java.util.Arrays;

/**
 * This class offers CLI interface for the project
 * @author Thamme Gowda
 */
public class Main {

    public static final Logger LOG = LoggerFactory.getLogger(Main.class);

    /**
     * known sub-commands
     */
    private enum Cmd {
        index("Index nutch segments to solr", SolrIndexer.class),
        phase2parse("Pharses the text content for NER and updates index", Phase2Indexer.class),
        graph("Builds a graph of documents, and writes the edges set to file ", GraphGenerator.class),
        pagerank("Computes page rank for nodes in graph", SparkPageRanker.class),
        updaterank("Updates Page rank", SolrPageRankUpdater.class);

        private final String description;
        private final Class<?> clazz;

        Cmd(String description, Class<?> clazz) {
            this.description = description;
            this.clazz =  clazz;
        }

        public String getDescription() {
            return description;
        }

        public Class<?> getClazz() {
            return clazz;
        }
    }

    public static Cmd getCommand(String cmdName) {
        try {
            return Cmd.valueOf(cmdName);
        } catch (Exception e) {
            System.err.println("Unknown command " + cmdName);
            printUsage(System.err);
            System.exit(2);
            throw new IllegalArgumentException("Unknown command " + cmdName);
        }
    }

    public static void printUsage(PrintStream out){
        out.println("Usage : Main <CMD>");
        out.println("The following command(CMD)s are available");
        for (Cmd cmd : Cmd.values()) {
            out.printf("%12s :  %s", cmd.name(), cmd.getDescription());
            out.println();
        }
        out.println();
        out.flush();
    }

    public static void main(String[] args) throws Exception {
        //args = "index -segs data/paths.txt -url http://localhost:8983/solr".split(" ");
        ///args = "phase2parse -batch 10 -src http://localhost:8983/solr/weapons1 -dest http://localhost:8983/solr/weapons3".split(" ");
        if (args.length == 0) {
            printUsage(System.out);
            System.exit(1);
        }
        Cmd cmd = getCommand(args[0]);  // the first argument has to be positional parma
        String subCmdArgs[] = new String[args.length-1];
        System.arraycopy(args, 1, subCmdArgs, 0, args.length - 1);

        Method mainMethod = cmd.getClazz().getDeclaredMethod("main", args.getClass());
        mainMethod.invoke(null, (Object) subCmdArgs);
    }
}
