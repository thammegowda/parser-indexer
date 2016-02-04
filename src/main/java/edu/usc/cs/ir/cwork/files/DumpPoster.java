package edu.usc.cs.ir.cwork.files;

import edu.usc.cs.ir.cwork.solr.ContentBean;
import edu.usc.cs.ir.cwork.tika.Parser;
import edu.usc.cs.ir.cwork.util.FileIterator;
import edu.usc.cs.ir.cwork.util.GroupedIterator;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrException;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by tg on 12/11/15.
 */
public class DumpPoster implements Runnable {

    public static final Logger LOG = LoggerFactory.getLogger(DumpPoster.class);

    @Option(name = "-in", usage = "Path to Files that are to be parsed and indexed", forbids = "-list")
    protected File file;

    @Option(name = "-list", usage = "Path Containing List of files to be processed", forbids = "-n")
    protected File listFile;


    @Option(name = "-solr", usage = "Solr URL", required = true)
    protected URL solrUrl;

    @Option(name = "-threads", usage = "Number of Threads")
    protected int nThreads = 5;

    @Option(name = "-timeout", usage = "task timeout. The parser should finish within this time millis")
    protected long threadTimeout = 15 * 1000;

    @Option(name = "-batch", usage = "Batch size for buffering solr postings")
    protected int batchSize = 500;

    protected ExecutorService service;

    /**
     * task for parsing docs
     */
    protected class ParseTask implements Callable<ContentBean> {

        private final Parser parser;
        private File inDoc;

        public ParseTask(File inDoc, Parser parser) {
            this.inDoc = inDoc;
            this.parser = parser;
        }

        @Override
        public ContentBean call() throws Exception {
            ContentBean outDoc = new ContentBean();
            parser.loadMetadataBean(inDoc, outDoc);
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

        Iterator<File> files = getInputFiles();
        long st = System.currentTimeMillis();
        long count = 0;
        long delay = 2 * 1000;

        HttpSolrServer destSolr = new HttpSolrServer(this.solrUrl.toString());
        destSolr.setConnectionTimeout(5*1000);

        GroupedIterator<File> groupedDocs = new GroupedIterator<>(files, nThreads);
        List<Future<ContentBean>> futures = new ArrayList<>(nThreads);
        List<ContentBean> buffer = new ArrayList<>();
        Parser parser = Parser.getInstance();
        while (groupedDocs.hasNext()) {
            try {
                List<File> group = groupedDocs.next();
                futures.clear();
                for (File doc : group) {
                    ParseTask task = new ParseTask(doc, parser);
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
                    } catch (InterruptedException e) {
                        LOG.error(e.getMessage());
                    } catch (ExecutionException e) {
                        LOG.error(e.getMessage(), e);
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
                    String lastPath = group.isEmpty() ? "EMPTY" : group.get(group.size() - 1).getPath();
                    LOG.info("Num Docs : {}, Last file: {}", count, lastPath);
                    st = System.currentTimeMillis();
                }
            } catch (SolrException e) {
                LOG.error(e.getMessage(), e);
                try {
                    LOG.warn("Going to sleep for sometime");
                    Thread.sleep(10000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
                LOG.warn("Woke Up! Going to add docs one by one");
                int errCount = 0;
                for (ContentBean bean : buffer) {
                    try {
                        destSolr.addBean(bean);
                    } catch (Exception e1) {
                        errCount++;
                        e1.printStackTrace();
                    }
                }
                LOG.info("Clearing the buffer. Errors :{}", errCount);
                //possibly an error in documents
                buffer.clear();
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

    private Iterator<File> getInputFiles() {
        if (file != null) {
            if (!file.exists()) {
                throw new IllegalArgumentException(file + " doesnt exists");
            }
            return new FileIterator(file);
        } else if (listFile != null ) {
            if (!listFile.exists()) {
                throw new IllegalArgumentException(listFile + " doesnt exists");
            }
            try {
                LineIterator iterator = FileUtils.lineIterator(listFile);

            return new Iterator<File>() {
                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public File next() {
                    return new File(iterator.next());
                }
            };
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new IllegalStateException("Error : file list or directory should be given");
        }
    }

    public static void main(String[] args) {
        //args = "-solr http://localhost:8983/solr/collection3 -in /home/tg/tmp/committer-index.html -batch 10".split(" ");

        DumpPoster poster = new DumpPoster();
        CmdLineParser parser = new CmdLineParser(poster);
        try {
            parser.parseArgument(args);
            if (poster.file == null && poster.listFile == null) {
                throw new CmdLineException(parser, "Either -in or -list is required");
            }
        } catch (CmdLineException e) {
            System.out.println(e.getMessage());
            parser.printUsage(System.out);
            return;
        }
        poster.run();
    }
}
