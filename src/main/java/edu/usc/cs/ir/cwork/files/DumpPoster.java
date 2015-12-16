package edu.usc.cs.ir.cwork.files;

import edu.usc.cs.ir.cwork.solr.ContentBean;
import edu.usc.cs.ir.cwork.tika.Parser;
import edu.usc.cs.ir.cwork.util.GroupedIterator;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.CanReadFileFilter;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by tg on 12/11/15.
 */
public class DumpPoster implements Runnable {

    public static final Logger LOG = LoggerFactory.getLogger(DumpPoster.class);

    @Option(name = "-in", usage = "Path to Files that are to be parsed and indexed", required = true)
    private File file;

    @Option(name = "-solr", usage = "Solr URL", required = true)
    private URL solrUrl;

    @Option(name = "-threads", usage = "Number of Threads")
    private int nThreads = 5;

    @Option(name = "-timeout", usage = "task timeout. The parser should finish within this time millis")
    private long threadTimeout = 15 * 1000;

    @Option(name = "-batch", usage = "Batch size for buffering solr postings")
    private int batchSize = 500;

    private ExecutorService service;

    /**
     * task for parsing docs
     */
    private class ParseTask implements Callable<ContentBean> {

        private File inDoc;

        public ParseTask(File inDoc) {
            this.inDoc = inDoc;
        }

        @Override
        public ContentBean call() throws Exception {
            ContentBean outDoc = new ContentBean();
            Parser.getInstance().loadMetadataBean(inDoc, outDoc);
            return outDoc;
        }
    }

    public synchronized ExecutorService getExecutors(){
        if (service == null) {
            this.service = Executors.newFixedThreadPool(nThreads);
        }
        return service;
    }

    @Override
    public void run() {
        if (!file.exists()) {
            throw new IllegalArgumentException(file + " doesnt exists");
        }

        Iterator<File> files;
        if (file.isFile()) {
            files = Collections.singletonList(file).iterator();
        } else {
            files = FileUtils.iterateFiles(file, CanReadFileFilter.CAN_READ, CanReadFileFilter.CAN_READ);
        }

        long st = System.currentTimeMillis();
        long count = 0;
        long delay = 2 * 1000;

        HttpSolrServer destSolr = new HttpSolrServer(this.solrUrl.toString());
        destSolr.setConnectionTimeout(5*1000);


        GroupedIterator<File> groupedDocs = new GroupedIterator<>(files, nThreads);
        List<Future<ContentBean>> futures = new ArrayList<>(nThreads);
        List<ContentBean> buffer = new ArrayList<>();
        while (groupedDocs.hasNext()) {
            try {
                List<File> group = groupedDocs.next();
                futures.clear();
                for (File doc : group) {
                    ParseTask task = new ParseTask(doc);
                    Future<ContentBean> future = getExecutors().submit(task);
                    futures.add(future);
                    count++;
                }

                // collect results
                for (Future<ContentBean> future : futures) {
                    try {
                        ContentBean result = future.get(threadTimeout, TimeUnit.MILLISECONDS);
                        if (result != null) {
                            buffer.add(result);
                        }
                    } catch (InterruptedException | ExecutionException e) {
                        LOG.error(e.getMessage());
                    } catch (TimeoutException e) {
                        // didnt finish
                        future.cancel(true);
                        LOG.warn("Cancelled a parse task, it didnt complete in time");
                    }
                }

                if (buffer.size() >= batchSize) {
                    destSolr.addBeans(buffer);
                    buffer.clear();
                }

                if (System.currentTimeMillis() - st > delay) {
                    LOG.info("Num Docs : {}", count);
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
        try {
            //left out
            if (!buffer.isEmpty()) {
                destSolr.addBeans(buffer);
            }
            LOG.info("Committing before exit. Num Docs = {}", count);
            UpdateResponse response = destSolr.commit();
            LOG.info("Commit response : {}", response);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (service != null) {
                System.out.println("Shutting down the thread pool");
                service.shutdown();
            }
        }
    }

    public static void main(String[] args) {
        //args = "-solr http://localhost:8983/solr/collection3 -in /home/tg/tmp".split(" ");

        DumpPoster poster = new DumpPoster();
        CmdLineParser parser = new CmdLineParser(poster);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            parser.printUsage(System.out);
        }
        poster.run();
    }
}
