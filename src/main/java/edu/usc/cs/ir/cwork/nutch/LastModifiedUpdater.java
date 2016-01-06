package edu.usc.cs.ir.cwork.nutch;

import edu.usc.cs.ir.cwork.solr.SolrDocUpdates;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.Content;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.function.Function;

/**
 *This class  was created to update 'Last-modified' value to solr
 */
public class LastModifiedUpdater implements Runnable, Function<Content, SolrInputDocument> {

    public static final Logger LOG = LoggerFactory.getLogger(LastModifiedUpdater.class);

    @Option(name="-list", usage = "File containing list of segments", required = true)
    private File segmentListFile;

    @Option(name = "-solr", usage = "Solr URL", required = true)
    private URL solrUrl;

    @Option(name="-dumpRoot", usage = "Path to root directory of nutch dump", required = true)
    private static String dumpDir = "/data2/";


    @Option(name = "-batch", usage = "Batch size")
    private int batchSize = 1000;

    private SolrServer solrServer;
    private Function<URL, String> pathFunction;

    private void init() throws MalformedURLException {
        solrServer = new HttpSolrServer(solrUrl.toString());
        pathFunction = new NutchDumpPathBuilder(dumpDir);
    }

    /**
     * Maps the nutch protocol content into solr input doc
     * @param content nutch content
     * @return solr input document
     * @throws Exception when an error happens
     */
    public SolrInputDocument apply(Content content) {
        String value = content.getMetadata().get(Response.LAST_MODIFIED);
        if (value == null) {
            return null;
        }
        SolrInputDocument doc = new SolrInputDocument();
        URL url;
        try {
            url = new URL(content.getUrl());
        } catch (MalformedURLException e) {
            e.printStackTrace();
            return null;
        }
        doc.addField("id", OutlinkUpdater.pathFunction.apply(url));

        doc.setField("lastmodified", new HashMap<String, Object>(){{
            put("set", value);}});

        doc.setField("dates", new HashMap<String, Object>(){{
            put("add", value);}});
        return doc;
}


    @Override
    public void run() {
        try {
            this.init();
            SolrDocUpdates updates = new SolrDocUpdates(this, this.segmentListFile);
            long count = OutlinkUpdater.indexAll(solrServer, updates, batchSize);
            System.out.println("Skipped : " + updates.getSkipCount());
            System.out.println("Count : " + count);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {

        //args = "-list /home/tg/tmp/seg.list -dumpRoot /data2/ -solr http://locahost:8983/solr/collection3".split(" ");
        LastModifiedUpdater generator = new LastModifiedUpdater();
        CmdLineParser parser = new CmdLineParser(generator);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.out.println(e.getMessage());
            e.getParser().printUsage(System.err);
            System.exit(1);
        }
        generator.run();
    }
}
