package edu.usc.cs.ir.cwork.nutch;

import com.google.common.io.Files;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ParseSegment;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.TableUtil;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
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
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by tg on 12/21/15.
 */
public class OutlinkUpdater implements Runnable {

    public static final Logger LOG = LoggerFactory.getLogger(OutlinkUpdater.class);

    @Option(name="-list", usage = "File containing list of segments", required = true)
    private File segmentListFile;

    @Option(name="-dumpRoot", usage = "Path to root directory of nutch dump", required = true)
    private static String dumpDir = "/data2/";

    @Option(name="-nutch", usage = "Path to nutch home directory. Hint: path to nutch/runtime/local", required = true)
    private File nutchHome;

    @Option(name = "-solr", usage = "Solr URL", required = true)
    private URL solrUrl;

    @Option(name = "-batch", usage = "Batch size")
    private int batchSize = 1000;

    private Configuration nutchConf;
    private ParseUtil parseUtil;
    private SolrServer solrServer;

    private void init() throws MalformedURLException {
        //Step 1: Nutch initialization
        nutchConf = NutchConfiguration.create();
        nutchConf.set("plugin.folders", new File(nutchHome, "plugins").getAbsolutePath());
        nutchConf.setInt("parser.timeout", 10);
        URLClassLoader loader = new URLClassLoader(
                new URL[]{ new File(nutchHome, "conf").toURI().toURL()},
                nutchConf.getClassLoader());
        nutchConf.setClassLoader(loader);
        parseUtil = new ParseUtil(nutchConf);

        //Step 2: initialize solr
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
     * Finds all segment content parts
     * @param directories list of segment directories
     * @return list of paths to segment content parts
     */
    public static List<String> findContentParts(List<String> directories)
            throws IOException, InterruptedException {
        List<String> paths = new ArrayList<>();
        String cmd[] = {"find", null, "-type", "f", "-regex", ".*/content/part-[0-9]+/data$"};
        for (String directory : directories) {
            cmd[1] = directory;
            LOG.info("Run : {}", Arrays.toString(cmd));
            Process process = Runtime.getRuntime().exec(cmd);
            //process.wait();
            List<String> lines = IOUtils.readLines(process.getInputStream());
            LOG.info("Found {} content parts in {}", lines.size(), directory);
            paths.addAll(lines);
        }
        return paths;
    }

    /**
     * Maps the nutch protocol content into solr input doc
     * @param content nutch content
     * @return solr input document
     * @throws Exception when an error happens
     */
    public SolrInputDocument getDocUpdate(Content content) throws Exception{
        if (ParseSegment.isTruncated(content)) {
            return null;
        }
        ParseResult result = parseUtil.parse(content);
        if (!result.isSuccess()) {
            return null;
        }
        Parse parsed = result.get(content.getUrl());
        if (parsed != null) {
            Outlink[] outlinks = parsed.getData().getOutlinks();
            if (outlinks != null && outlinks.length > 0) {

                SolrInputDocument doc = new SolrInputDocument();
                URL url = new URL(content.getUrl());
                doc.addField("id", pathFunction.apply(url));
                doc.addField("url", new HashMap<String, String>(){{put("set", url.toString());}});
                doc.setField("host", new HashMap<String, String>(){{put("set", url.getHost());}});
                List<String> links = new ArrayList<>();
                List<String> paths = new ArrayList<>();
                HashSet<String> uniqOutlinks = new HashSet<>();
                for (Outlink outlink : outlinks) {
                    uniqOutlinks.add(outlink.getToUrl());
                }
                for (String link : uniqOutlinks) {
                    links.add(link);
                    paths.add(pathFunction.apply(new URL(link)));
                }
                doc.setField("outlinks", new HashMap<String, Object>(){{
                    put("set", links);}});
                doc.setField("outpaths", new HashMap<String, Object>(){{
                    put("set", paths);}});
                return doc;
            }
        } else {
            System.err.println("This shouldn't be happening");
        }
        return null;
    }


    /**
     * This iterator creates a stream of solr updates by reading the input segment paths
     */
    private static class SolrDocUpdates implements Iterator<SolrInputDocument> {

        private static final Logger LOG = LoggerFactory.getLogger(SolrDocUpdates.class);
        private SolrInputDocument next;
        private OutlinkUpdater generator;
        private final RecordIterator<Content> input;
        private boolean skipImages = true;

        public SolrDocUpdates(OutlinkUpdater generator) throws IOException, InterruptedException {
            this.generator = generator;
            List<String> segments = Files.readLines(generator.segmentListFile, Charset.defaultCharset());
            List<String> parts = findContentParts(segments);
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
                    if (skipImages && content.getValue().getContentType()
                            .toLowerCase().startsWith("image")) {
                        //skip Images
                        continue;
                    }

                    SolrInputDocument update = generator.getDocUpdate(content.getValue());
                    if (update != null) {
                        return update;
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


    /**
     * Indexes all the documents in the stream to solr
     * @param solr the solr server
     * @param docsStream input doc stream
     * @param bufferSize buffer size
     * @return number of documents indexed
     */
    public static long indexAll(SolrServer solr,
                         Iterator<SolrInputDocument> docsStream,
                         int bufferSize) {
        List<SolrInputDocument> buffer = new ArrayList<>(bufferSize);

        long count = 0;
        int printDelay = 2 * 1000;
        long t1 = System.currentTimeMillis();
        while(docsStream.hasNext()) {

            buffer.add(docsStream.next());
            count++;

            if (buffer.size() >= bufferSize) {
                // process
                try {
                    solr.add(buffer);
                } catch (SolrServerException e) {
                    try {
                        Thread.sleep(10*1000);
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                    //One more attempt: add one by one
                    for (SolrInputDocument document : buffer) {
                        try {
                            solr.add(document);
                        } catch (Exception e1) {
                            e1.printStackTrace();
                        }
                    }
                } catch (IOException e) {
                    LOG.error(e.getMessage(), e);
                    try {
                        Thread.sleep(10*1000);
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                }

                //empty the buffer.
                buffer.clear();
            }

            if (System.currentTimeMillis() - t1 > printDelay) {
                t1 = System.currentTimeMillis();
                LOG.info("Num docs : {}", count);
            }
        }

        if (!buffer.isEmpty()) {
            //process left out docs in buffer
            try {
                solr.add(buffer);
            } catch (SolrServerException | IOException e) {
                e.printStackTrace();
            }
        }
        try {
            LOG.info("End || Count:: {}", count);
            LOG.info("Committing:: {}", solr.commit());
        } catch (SolrServerException | IOException e) {
            e.printStackTrace();
        }
        return count;
    }

    @Override
    public void run() {

        try {
            this.init();
            SolrDocUpdates updates = new SolrDocUpdates(this);
            indexAll(solrServer, updates, batchSize);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {

        //args = "-list /home/tg/tmp/seg.list -dumpRoot /data2 -nutch /home/tg/work/coursework/cs572/nutch -solr http://locahost:8983/solr/collection3".split(" ");
        OutlinkUpdater generator = new OutlinkUpdater();
        CmdLineParser parser = new CmdLineParser(generator);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.out.println(e.getMessage());
            e.getParser().printUsage(System.err);
            System.exit(1);
        }
        generator.init();
        generator.run();
    }
}
