package edu.usc.cs.ir.cwork.files;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import edu.usc.cs.ir.cwork.nutch.OutlinkUpdater;
import edu.usc.cs.ir.cwork.solr.ContentBean;
import edu.usc.cs.ir.cwork.tika.Parser;
import edu.usc.cs.ir.cwork.util.GroupedIterator;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by tg on 12/11/15.
 */
public class DarkDumpPoster extends DumpPoster {

    public static final Logger LOG = LoggerFactory.getLogger(DarkDumpPoster.class);
    public static final Set<String> TAGS = new HashSet<String>() {{add("dark");}};

    private static class LinkRecord{
        public String domain;
        public String url;
        public String title;
        public String path;
    }

    private class LinkRecParseTask extends ParseTask{

        public static final String TEXT_HTML = "text/html";
        private LinkRecord rec;

        public LinkRecParseTask(LinkRecord inDoc, Parser parser) {
            super(new File(inDoc.path), parser);
            this.rec = inDoc;
        }

        @Override
        public ContentBean call() throws Exception {
            ContentBean bean = super.call();
            File file = new File(rec.path);
            bean.setId(file.toURI().toString());
            bean.setUrl(rec.url);
            bean.setTitle(rec.title);
            bean.getMetadata().put("source_data_ss_md", TAGS);
            byte[] bytes = Files.readAllBytes(file.toPath());
            Content content = new Content(rec.url, rec.url, bytes,
                    TEXT_HTML, new Metadata(), nutchConf);
            ParseResult result = parseUtil.parse(content);

            Parse parsed = result.get(content.getUrl());

            if (parsed != null) {
                Outlink[] outlinks = parsed.getData().getOutlinks();
                if (outlinks != null && outlinks.length > 0) {

                    HashSet<String> uniqLinks = new HashSet<>();
                    bean.setOutlinks(uniqLinks);
                    bean.setOutpaths(new HashSet<>());
                    for (Outlink outlink : outlinks) {
                        uniqLinks.add(outlink.getToUrl());
                    }
                    for (String link : uniqLinks) {
                        String path = pathFunction.apply(link);
                        if (path != null) {
                            bean.getOutpaths().add(path);
                        }

                    }
                } else {
                    System.err.println("No outlinks found");
                }
            } else {
                System.err.println("This shouldn't be happening");
            }
            return bean;
        }
    }

    @Option(name="-nutch", usage = "Path to nutch home directory. Hint: path to nutch/runtime/local", required = true)
    private File nutchHome;

    private SolrServer solr;
    private ParseUtil parseUtil;
    private Configuration nutchConf;
    private Function<String, String> pathFunction;
    private Map<String, String> url2PathIdx;


    private void init(){
        try {
            this.solr = new HttpSolrServer(this.solrUrl.toString());
            //Step 1: Nutch initialization
            nutchConf = NutchConfiguration.create();
            nutchConf.set("plugin.folders", new File(nutchHome, "plugins").getAbsolutePath());
            nutchConf.setInt("parser.timeout", 10);
            URLClassLoader loader = new URLClassLoader(
                    new URL[]{new File(nutchHome, "conf").toURI().toURL()},
                    nutchConf.getClassLoader());
            nutchConf.setClassLoader(loader);
            parseUtil = new ParseUtil(nutchConf);
            LOG.info("Creating the inmemory index for url to path");
            this.url2PathIdx = new HashMap<>();
            CSVParser csvRecords = CSVFormat.DEFAULT.parse(new FileReader(this.listFile));
            for (CSVRecord record : csvRecords) {
                url2PathIdx.put(record.get(1), record.get(4));
            }
            LOG.info("Index has {} recs", url2PathIdx.size());
            pathFunction = new Function<String, String>() {
                @Nullable
                @Override
                public String apply(@Nullable String input) {
                    return url2PathIdx.get(input);
                }
            };

            //FIXME: set path function
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Iterator<LinkRecord> getRecords() {
        CSVParser parser;
        try {
            parser = CSVFormat.DEFAULT.parse(new FileReader(listFile));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return Iterators.transform(parser.iterator(),
                new Function<CSVRecord, LinkRecord>() {
            @Nullable
            @Override
            public LinkRecord apply(@Nullable CSVRecord input) {
                if (input != null) {
                    LinkRecord rec = new LinkRecord();
                    rec.domain = input.get(0);
                    rec.url = input.get(1);
                    rec.title = input.get(2);
                    rec.path = input.get(4);
                    return rec;
                }
                return null;
            }
        });
    }

    @Override
    public void run() {
        long st = System.currentTimeMillis();
        long count = 0;
        long delay = 2 * 1000;

        Iterator<LinkRecord> records = getRecords();
        GroupedIterator<LinkRecord> groupedDocs = new GroupedIterator<>(records, nThreads);
        List<Future<ContentBean>> futures = new ArrayList<>(nThreads);
        List<ContentBean> buffer = new ArrayList<>();
        Parser parser = Parser.getInstance();
        while (groupedDocs.hasNext()) {
            try {
                List<LinkRecord> group = groupedDocs.next();
                futures.clear();
                for (LinkRecord doc : group) {
                    LinkRecParseTask task = new LinkRecParseTask(doc, parser);
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
                        LOG.error(e.getMessage(), e);
                    } catch (TimeoutException e) {
                        // didnt finish
                        future.cancel(true);
                        LOG.warn("Cancelled a parse task, it didnt complete in time");
                    }
                }

                if (buffer.size() >= batchSize) {
                    solr.addBeans(buffer);
                    buffer.clear();
                }

                if (System.currentTimeMillis() - st > delay) {
                    String lastPath = group.isEmpty() ? "EMPTY" : group.get(group.size() - 1).path;
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
                        solr.addBean(bean);
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
                solr.addBeans(buffer);
            }
            LOG.info("Committing before exit. Num Docs = {}", count);
            UpdateResponse response = solr.commit();
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
        //args = "-solr http://localhost:8983/solr/collection3 -in /home/tg/tmp/committer-index.html -batch 10".split(" ");

        DarkDumpPoster poster = new DarkDumpPoster();
        CmdLineParser parser = new CmdLineParser(poster);
        try {
            parser.parseArgument(args);
            if (poster.listFile == null) {
                throw new CmdLineException(parser, "-list is required.");
            }
        } catch (CmdLineException e) {
            System.out.println(e.getMessage());
            parser.printUsage(System.out);
            return;
        }
        poster.init();
        poster.run();
    }
}
