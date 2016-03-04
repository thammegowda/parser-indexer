package edu.usc.cs.ir.cwork.es;

import edu.usc.cs.ir.cwork.nutch.NutchDumpPathBuilder;
import edu.usc.cs.ir.cwork.nutch.RecordIterator;
import edu.usc.cs.ir.cwork.nutch.SegContentReader;
import edu.usc.cs.ir.cwork.solr.ContentBean;
import edu.usc.cs.ir.cwork.tika.Parser;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import org.apache.commons.io.IOUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.nutch.protocol.Content;
import org.apache.solr.client.solrj.SolrServerException;
import org.json.JSONObject;
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
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * This class accepts CLI args containing paths to Nutch segments and elastic search,
 * then runs the indexes the documents to elastic search by parsing the metadata.
 */
public class EsIndexer {


    private static Logger LOG = LoggerFactory.getLogger(EsIndexer.class);

    @Option(name = "-segs", usage = "Path to a text file containing segment paths. One path per line",
            required = true)
    private File segsFile;

    @Option(name = "-ehost", usage = "ES Host", required = true)
    private String elasticHost;

    @Option(name = "-eport", usage = "ES port")
    private int elasticPost = 9300;

    @Option(name = "-ecluster", usage = "ES clustername")
    private String elasticCluster = "elasticsearch";

    @Option(name = "-nutch", usage = "Path to Nutch home.", required = true)
    private String nutchHome;

    @Option(name = "-dump", usage = "Path to dump prefix.", required = true)
    private String dumpPath;

    @Option(name = "-batch", usage = "Number of documents to buffer and post to solr",
            required = false)
    private int batchSize = 1000;

    @Option(name= "-cdrcreds", usage = "CDR credentials properties file.", required = true)
    private File cdrCredsFile;

    private CDRCreds creds;
    private Function<URL, String> pathMapper;


    /**
     * This POJO stores MEMEX credentials
     */
    public static class CDRCreds {
        public String clusterUri;
        public String username;
        public String password;
        public String indexType;
        public String indexName;

        public CDRCreds(Properties props){
            this.clusterUri = props.getProperty("memex.cdr.cluster");
            this.username = props.getProperty("memex.cdr.username");
            this.password = props.getProperty("memex.cdr.password");
            this.indexName = props.getProperty("memex.cdr.index");
            this.indexType = props.getProperty("memex.cdr.type");
        }
    }
    /**
     * runs the solr index command
     * @throws IOException
     * @throws InterruptedException
     * @throws SolrServerException
     */
    public void run() throws IOException, InterruptedException, SolrServerException {

        //STEP 1: initialize NUTCH
        System.setProperty("nutch.home", nutchHome);
        //step 2: path mapper
        this.pathMapper = new NutchDumpPathBuilder(this.dumpPath);

        //Step
        LOG.info("Getting cdr details from {}", cdrCredsFile);
        Properties props = new Properties();
        props.load(new FileInputStream(cdrCredsFile));
        this.creds = new CDRCreds(props);

        JestClient client = openCDRClient();
        try {
            //Step
            FileInputStream stream = new FileInputStream(segsFile);
            List<String> paths = IOUtils.readLines(stream);
            IOUtils.closeQuietly(stream);
            LOG.info("Found {} lines in {}", paths.size(), segsFile.getAbsolutePath());
            SegContentReader reader = new SegContentReader(paths);
            RecordIterator recs = reader.read();

            //Step 4: elastic client
            index(recs, client);
            System.out.println(recs.getCount());
        }finally {
            LOG.info("Shutting down jest client");
            client.shutdownClient();
        }
    }

    private JestClient openCDRClient(){
        LOG.info("CDR name:type = {}:{}", creds.indexName, creds.indexType);
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig.
                Builder(creds.clusterUri).discoveryEnabled(false).discoveryFrequency(1l, TimeUnit.MINUTES).multiThreaded(true)
                .defaultCredentials(creds.username, creds.password)
                .connTimeout(300000).readTimeout(300000)
                .build());
        return factory.getObject();
    }

    private void index(RecordIterator recs, JestClient elastic)
            throws IOException, SolrServerException {

        long st = System.currentTimeMillis();
        long count = 0;
        long delay = 2 * 1000;
        Parser parser = Parser.getInstance();
        List<JSONObject> buffer = new ArrayList<>(batchSize);
        while (recs.hasNext()) {
            Pair<String, Content> rec = recs.next();
            Content content = rec.getValue();
            ContentBean bean = new ContentBean();
            try {
                parser.loadMetadataBean(content, pathMapper, bean);
                buffer.add(ESMapper.toCDRSchema(bean));
                count++;
                if (buffer.size() >= batchSize) {
                    try {
                        indexAll(buffer, elastic);
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
            indexAll(buffer, elastic);
            buffer.clear();
        }
        LOG.info("Num Docs = {}", count);
    }

    private void indexAll(List<JSONObject> docs, JestClient client) throws IOException {

        List<Index> inputDocs = new ArrayList<>();
        for (JSONObject doc : docs) {
            String id = (String) doc.remove("obj_id");
            if (id == null) {
                LOG.warn("No ID set to document. Skipped");
                continue;
            }
            inputDocs.add(new Index.Builder(doc.toString()).id(id).build());
        }
        Bulk bulk = new Bulk.Builder()
                .defaultIndex(creds.indexName)
                .defaultType(creds.indexType)
                .addAction(inputDocs)
                .build();
        JestResult result = client.execute(bulk);
        if (!result.isSucceeded()){
            LOG.error("Failure in bulk commit: {}", result.getErrorMessage());
        }
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
        LOG.info("Done");
        System.out.println("Done");
    }
}
