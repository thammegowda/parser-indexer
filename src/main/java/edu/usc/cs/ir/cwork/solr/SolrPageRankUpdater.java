package edu.usc.cs.ir.cwork.solr;

/**
 * Created by nmante on 11/4/15.
 */

import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * This class accepts CLI args containing paths to Nutch segments and solr Url,
 * then runs the index operation optionally reparsing the metadata.
 */
public class SolrPageRankUpdater {

    private static Logger LOG = LoggerFactory.getLogger(SolrPageRankUpdater.class);

    @Option(name="-solr", aliases = {"--solr-url"}, required = true,
        usage = "Solr server URL")
    private URL solrUrl;

    @Option(name="-ranks", aliases = {"--ranks-file"}, required = true,
    usage = "File containing Page ranks. Each line should have 'URL\t(double)SCORE'")
    private File ranksFile;

    @Option(name="-field", aliases = {"--rank-field"}, required = true,
            usage = "Solr schema field for storing the page rank")
    private String rankField;

    @Option(name="-batch", aliases = {"--batch-size"},
            usage = "Batch or buffer size")
    private int batchSize = 1000;

    /**
     * Updates documents in solr with pageranks
     *
     * @throws IOException
     * @throws SolrServerException
     */
    public void run() throws IOException, SolrServerException{
        // create the SolrJ Server
        SolrServer solr = new HttpSolrServer(solrUrl.toString());

        LOG.info("Reading page ranks from {}", rankField);
        try (InputStream stream = new FileInputStream(ranksFile)) {
            Iterator<String> lines = IOUtils.lineIterator(stream, StandardCharsets.UTF_8);

            ArrayList<SolrInputDocument> buffer = new ArrayList<>(batchSize);
            long st = System.currentTimeMillis();
            long count = 0;
            long delay = 2 * 1000;

            while (lines.hasNext()) {
                String line = lines.next().trim();
                if (line.isEmpty()) {
                    continue;
                }
                String[] parts = line.split("\\s");
                String id = parts[0];
                Double score = Double.valueOf(parts[1]);


                // create the document
                SolrInputDocument sDoc = new SolrInputDocument();
                // Add the id field, and page rank field to the document
                sDoc.addField("id", id);
                Map<String, Double> fieldModifier = new HashMap<>();
                fieldModifier.put("set", score);
                sDoc.addField(rankField, fieldModifier);  // add the map as the field value
                buffer.add(sDoc);

                count++;
                if (buffer.size() >= batchSize) {
                    try {
                        solr.add(buffer);
                        buffer.clear();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                if (System.currentTimeMillis() - st > delay) {
                    LOG.info("Num Docs : {}", count);
                    st = System.currentTimeMillis();
                }
            }
            //left out
            if (!buffer.isEmpty()) {
                solr.add(buffer);
            }

            // commit
            LOG.info("Committing before exit. Num Docs = {}", count);
            UpdateResponse response = solr.commit();
            solr.shutdown();
            LOG.info("Commit response : {}", response);
        }

    }

    public static void main(String[] args) throws InterruptedException,
            SolrServerException, IOException {

        SolrPageRankUpdater updater= new SolrPageRankUpdater();
        CmdLineParser cmdLineParser = new CmdLineParser(updater);
        try {
            cmdLineParser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            cmdLineParser.printUsage(System.out);
            return;
        }
        updater.run();
        System.out.println("Done");
    }
}
