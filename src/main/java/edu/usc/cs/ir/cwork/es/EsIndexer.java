package edu.usc.cs.ir.cwork.es;

import edu.usc.cs.ir.cwork.nutch.NutchDumpPathBuilder;
import edu.usc.cs.ir.cwork.nutch.RecordIterator;
import edu.usc.cs.ir.cwork.nutch.SegContentReader;
import edu.usc.cs.ir.cwork.solr.ContentBean;
import edu.usc.cs.ir.cwork.tika.Parser;
import org.apache.commons.io.IOUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.protocol.Content;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * This class accepts CLI args containing paths to Nutch segments and solr Url,
 * then runs the index operation optionally reparsing the metadata.
 */
public class EsIndexer {

    public static final String MD_SUFFIX = "_md";
    private static Logger LOG = LoggerFactory.getLogger(EsIndexer.class);
    private static Set<String> TEXT_TYPES = new HashSet<>(Arrays.asList("html", "xhtml", "xml", "plain", "xhtml+xml"));

    @Option(name = "-segs", usage = "Path to a text file containing segment paths. One path per line",
            required = true)
    private File segsFile;

    @Option(name = "-url", usage = "ES Url", required = true)
    private URL solrUrl;

    @Option(name = "-nutch", usage = "Path to Nutch home.", required = true)
    private String nutchHome;

    @Option(name = "-dump", usage = "Path to dump prefix.", required = true)
    private String dumpPath;

    @Option(name = "-batch", usage = "Number of documents to buffer and post to solr",
            required = false)
    private int batchSize = 1000;
    private Function<URL, String> pathMapper;



    /**
     * runs the solr index command
     * @throws IOException
     * @throws InterruptedException
     * @throws SolrServerException
     */
    public void run() throws IOException, InterruptedException, SolrServerException {

        System.setProperty("nutch.home", nutchHome);
        this.pathMapper = new NutchDumpPathBuilder(this.dumpPath);

        FileInputStream stream = new FileInputStream(segsFile);
        List<String> paths = IOUtils.readLines(stream);
        IOUtils.closeQuietly(stream);
        LOG.info("Found {} lines in {}", paths.size(), segsFile.getAbsolutePath());
        SegContentReader reader = new SegContentReader(paths);
        RecordIterator recs = reader.read();
        index(recs, TODO:elastic);
        System.out.println(recs.getCount());
    }

    private void index(RecordIterator recs,  TODO:elastic) throws IOException, SolrServerException {

        long st = System.currentTimeMillis();
        long count = 0;
        long delay = 2 * 1000;
        Parser parser = Parser.getInstance();
        List<NutchDocument> buffer = new ArrayList<>(batchSize);
        while (recs.hasNext()) {
            Pair<String, Content> rec = recs.next();
            Content content = rec.getValue();
            ContentBean bean = new ContentBean();
            try {
                parser.loadMetadataBean(content, pathMapper, bean);
                buffer.add(ESMapper.beanToNutchDoc(bean));
                count++;
                if (buffer.size() >= batchSize) {
                    try {
                        TODO:
                        elastic.addBeans(buffer);
                        buffer.clear();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                if (System.currentTimeMillis() - st > delay) {
                    LOG.info("Num Docs : {}", count);
                    st = System.currentTimeMillis();
                }
            } catch (Exception e){
                LOG.error("Error processing {}", content.getUrl());
            }
        }

        //left out
        if (!buffer.isEmpty()) {
            TODO:elastic.add(buffer);
        }

        // commit
        LOG.info("Committing before exit. Num Docs = {}", count);
        UpdateResponse response = TODO:elastic.commit();
        LOG.info("Commit response : {}", response);
    }

    public static void main(String[] args) throws InterruptedException,
            SolrServerException, IOException {
        EsIndexer indexer = new EsIndexer();
        CmdLineParser cmdLineParser = new CmdLineParser(indexer);
        try {
            cmdLineParser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            cmdLineParser.printUsage(System.out);
            return;
        }

        indexer.run();
        System.out.println("Done");
    }
}
