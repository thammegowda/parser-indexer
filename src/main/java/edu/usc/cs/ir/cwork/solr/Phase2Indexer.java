package edu.usc.cs.ir.cwork.solr;

import edu.usc.cs.ir.cwork.solr.schema.FieldMapper;
import edu.usc.cs.ir.cwork.tika.Parser;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.tika.metadata.Metadata;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This class accepts CLI args containing paths to Nutch segments and solr Url,
 * then runs the index operation optionally reparsing the metadata.
 */
public class Phase2Indexer {

    private static final Logger LOG = LoggerFactory.getLogger(SolrIndexer.class);
    public static final String MD_SUFFIX = "_md";
    public static final FieldMapper mapper = FieldMapper.create();
    public static final Map<String, String> map = new HashMap<>();
    {
        map.put("NER_PERSON", "persons");
        map.put("NER_LOCATION", "locations");
        map.put("NER_ORGANIZATION", "organizations");
        map.put("NER_PHONE_NUMBER", "phonenumbers");
        map.put("NER_WEAPON_NAME", "weaponnames");
        map.put("NER_WEAPON_TYPE", "weapontypes");
    }


    @Option(name = "-src", aliases = {"--src-solr"},
            usage = "Source Solr url", required = true)
    private URL srcSolr;

    @Option(name = "-dest", aliases = {"--dest-solr"},
            usage = "Destination Solr url", required = true)
    private URL destSolr;

    @Option(name = "-batch", aliases = {"--batch-size"},
            usage = "Number of documents to buffer and post to solr",
            required = false)
    private int batchSize = 1000;

    @Option(name = "-start", aliases = {"--start"},
            usage = "Import start",
            required = false)
    private int start = 0;


    @Option(name = "-q", aliases = {"--query"},
            usage = "Import Query",
            required = false)
    private String queryStr = "*:*";

    @Option(name = "-threads", aliases = {"--threads"},
            usage = "Number of Threads",
            required = false)
    private int nThreads = 2;


    @Option(name = "-timeout",
            usage = "Time out for parser in millis",
            required = false)
    private int threadTimeout = 1500;

    private ExecutorService service;

    private String[] copyFields = {"id", "title", "content",
            "contentLength", "boost", "lastModified", "digest", "host"};

    private static Set<String> textFields = new HashSet<>(
            Arrays.asList("id", "title", "content", "lastModified"));


    public synchronized ExecutorService getExecutors(){
        if (service == null) {
            this.service = Executors.newFixedThreadPool(nThreads);
        }
        return service;
    }

    /**
     * runs the solr index command
     * @throws IOException
     * @throws InterruptedException
     * @throws SolrServerException
     */
    public void run() throws Exception {

        HttpSolrServer solrServer = new HttpSolrServer(srcSolr.toString());
        solrServer.setConnectionTimeout(5*1000);
        SolrDocIterator docs = new SolrDocIterator(solrServer, queryStr,
                start, batchSize, copyFields);
        parseAndUpdate(docs);

    }

    private static class ParseTask implements Callable<Boolean> {

        private SolrDocument inDoc;
        private SolrInputDocument outDoc;

        public ParseTask(SolrDocument inDoc, SolrInputDocument doc) {
            this.inDoc = inDoc;
            this.outDoc = doc;
        }

        @Override
        public Boolean call() throws Exception {
            StringBuilder sb = new StringBuilder();
            for (String field : inDoc.getFieldNames()) {
                outDoc.setField(field, inDoc.get(field)); //copy
                if (textFields.contains(field)) {
                    sb.append(inDoc.get(field)).append("\n");
                }
            }
            String text = sb.toString();
            Metadata md = Parser.getPhase2Parser().parseContent(text);
            for (String name : md.names()) {
                Serializable value = md.isMultiValued(name) ?
                        md.getValues(name) : md.get(name);
                if (map.containsKey(name)) { //mapping exists
                    outDoc.setField(map.get(name), value);
                } else {
                    String newName = mapper.mapField(name, value);
                    if (newName != null) {
                        newName += MD_SUFFIX;
                        outDoc.setField(newName, value);
                    }
                }
            }

            Set<Date> dates = Parser.parseDates(text);
            if (dates != null && !dates.isEmpty()) {
                outDoc.addField("dates", dates);
            }
            return true;
        }
    }


    /**
     * This is a decorator iterator which creates groups from stream.
     * This is useful for parallelizing using threads.
     */
    private static class GroupedIterator implements Iterator<List<SolrDocument>> {

        private SolrDocIterator iterator;
        private int groupSize;

        private List<SolrDocument> next;

        public GroupedIterator(SolrDocIterator iterator, int groupSize) {
            this.iterator = iterator;
            this.groupSize = groupSize;
            this.next = getNext();
        }

        private List<SolrDocument> getNext(){
            List<SolrDocument> list = null;
            if (iterator != null && iterator.hasNext()) {
                int count = 0;
                list = new ArrayList<>(groupSize);
                while (count < groupSize && iterator.hasNext()) {
                    list.add(iterator.next());
                    count++;
                }
            }
            return list;
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public List<SolrDocument> next() {
            List<SolrDocument> tmp = this.next;
            this.next = getNext();
            return tmp;
        }
    }

    private void parseAndUpdate(SolrDocIterator docs)
            throws IOException, SolrServerException {

        long st = System.currentTimeMillis();
        long count = 0;
        long delay = 2 * 1000;

        HttpSolrServer destSolr = new HttpSolrServer(this.destSolr.toString());
        destSolr.setConnectionTimeout(5*1000);

        List<SolrInputDocument> buffer = new ArrayList<>(batchSize);
        GroupedIterator groupedDocs = new GroupedIterator(docs, nThreads);
        List<Future<Boolean>> futures = new ArrayList<>(nThreads);
        while (groupedDocs.hasNext()) {
            try {
                List<SolrDocument> group = groupedDocs.getNext();
                futures.clear();
                for (SolrDocument doc : group) {
                    SolrInputDocument delta = new SolrInputDocument();
                    delta.setField("id", doc.get("id"));
                    buffer.add(delta);
                    ParseTask task = new ParseTask(doc, delta);
                    Future<Boolean> future = getExecutors().submit(task);
                    futures.add(future);
                    count++;
                }

                // collect results
                for (Future<Boolean> future : futures) {
                    try {
                        Boolean result = future.get(threadTimeout, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException | ExecutionException e) {
                        LOG.error(e.getMessage());
                    } catch (TimeoutException e) {
                        // didnt finish
                        future.cancel(true);
                        LOG.warn("Cancelled a parse task, it didnt complete in time");
                    }
                }

                if (buffer.size() >= batchSize) {
                    destSolr.add(buffer);
                    buffer.clear();
                }

                if (System.currentTimeMillis() - st > delay) {
                    LOG.info("Num Docs : {},  Imported {} of {}", count,
                            docs.getNextStart(), docs.getNumFound());
                    st = System.currentTimeMillis();
                }
            } catch (Exception e){
                LOG.error(e.getMessage(), e);
                try {
                    Thread.sleep(4000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
        //left out
        if (!buffer.isEmpty()) {
            destSolr.add(buffer);
        }
        LOG.info("Committing before exit. Num Docs = {}", count);
        UpdateResponse response = destSolr.commit();
        LOG.info("Commit response : {}", response);
    }

    public static void main(String[] args) throws Exception {

        //args = "-batch 10 -src http://localhost:8983/solr/weapons1 -dest http://localhost:8983/solr/collection1".split(" ");
        //args = "-src http://localhost:8983/solr/weapons1 -dest http://localhost:8983/solr/collection2 -q id:\"http://tucson.americanlisted.com/cars/1997-buick-lesabre-cust_32547135.html\"".split(" ");
        Phase2Indexer indexer = new Phase2Indexer();
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
