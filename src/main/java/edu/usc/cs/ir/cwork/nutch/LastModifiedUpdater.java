package edu.usc.cs.ir.cwork.nutch;

import com.google.common.io.Files;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.TableUtil;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 *This class  was created to update 'Last-modified' value to solr
 */
public class LastModifiedUpdater implements Runnable {

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

    private void init() throws MalformedURLException {
        solrServer = new HttpSolrServer(solrUrl.toString());
    }


    public static Function<URL, String> pathFunction = url -> {

        String[] reversedURL = TableUtil.reverseUrl(url).split(":");
        reversedURL[0] = reversedURL[0].replace('.', '/');

        String reversedURLPath = reversedURL[0] + "/" + DigestUtils.sha256Hex(url.toString()).toUpperCase();
        String pathStr = String.format("%s/%s", dumpDir, reversedURLPath);
        return new File(pathStr).toURI().toString();
    };
    /**
     * Maps the nutch protocol content into solr input doc
     * @param content nutch content
     * @return solr input document
     * @throws Exception when an error happens
     */
    public SolrInputDocument getDocUpdate(Content content) throws Exception{
        String value = content.getMetadata().get(Response.LAST_MODIFIED);
        if (value == null) {
            return null;
        }
        SolrInputDocument doc = new SolrInputDocument();
        URL url = new URL(content.getUrl());
        doc.addField("id", OutlinkUpdater.pathFunction.apply(url));

        doc.setField("lastmodified", new HashMap<String, Object>(){{
            put("set", value);}});

        doc.setField("dates", new HashMap<String, Object>(){{
            put("add", value);}});
        return doc;
}


/**
 * This iterator creates a stream of solr updates by reading the input segment paths
 */
private static class SolrDocUpdates implements Iterator<SolrInputDocument> {

    private static final Logger LOG = LoggerFactory.getLogger(SolrDocUpdates.class);
    private SolrInputDocument next;
    private LastModifiedUpdater generator;
    private final RecordIterator<Content> input;
    private long skipped;

    public SolrDocUpdates(LastModifiedUpdater generator) throws IOException, InterruptedException {
        this.generator = generator;
        List<String> segments = Files.readLines(generator.segmentListFile, Charset.defaultCharset());
        List<String> parts = OutlinkUpdater.findContentParts(segments);
        List<Path> paths = parts.stream().map(Path::new).collect(Collectors.toList());

        System.out.println("Found " + segments.size() + " segments");
        System.out.println("Found " + paths.size() + " parts");
        input = new RecordIterator<>(paths);
        next = makeNext();
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public SolrInputDocument next() {
        SolrInputDocument tmp = next;
        next = makeNext();
        return tmp;
    }

    private SolrInputDocument makeNext() {
        while (input.hasNext()) {
            try {
                Pair<String, Content> content = input.next();
                SolrInputDocument update = generator.getDocUpdate(content.getValue());
                if (update != null) {
                    return update;
                } else {
                    skipped++;
                }
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }
        //end
        return null;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Remove not allowed");
    }
}



    @Override
    public void run() {
        try {
            this.init();
            SolrDocUpdates updates = new SolrDocUpdates(this);
            OutlinkUpdater.indexAll(solrServer, updates, batchSize);
            AtomicInteger i = new AtomicInteger(0);
            updates.forEachRemaining(u-> {
                System.out.println(u);
                i.incrementAndGet();
            });
            System.out.println("Skipped : " + updates.skipped);
            System.out.println("Count : " + i.get());
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
