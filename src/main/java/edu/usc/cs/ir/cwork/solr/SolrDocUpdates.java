package edu.usc.cs.ir.cwork.solr;

import com.google.common.io.Files;
import edu.usc.cs.ir.cwork.nutch.OutlinkUpdater;
import edu.usc.cs.ir.cwork.nutch.RecordIterator;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.nutch.protocol.Content;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This iterator creates a stream of solr updates by reading the input segment paths
 */
public class SolrDocUpdates implements Iterator<SolrInputDocument> {

    private static final Logger LOG = LoggerFactory.getLogger(SolrDocUpdates.class);
    private SolrInputDocument next;
    private Function<Content, SolrInputDocument> transformer;
    private final RecordIterator<Content> input;
    private long skipCount;
    private boolean skipImages;

    public SolrDocUpdates(Function<Content, SolrInputDocument> transformer,
                          File segmentListFile) throws IOException, InterruptedException {
        this.transformer = transformer;
        List<String> segments = Files.readLines(segmentListFile, Charset.defaultCharset());
        List<String> parts = OutlinkUpdater.findContentParts(segments);
        List<Path> paths = parts.stream().map(Path::new).collect(Collectors.toList());

        System.out.println("Found " + segments.size() + " segments");
        System.out.println("Found " + paths.size() + " parts");
        input = new RecordIterator<>(paths);
        next = makeNext();
    }

    /**
     * Gets number of docs skipped due to errors
     * @return num of skipped docs
     */
    public long getSkipCount() {
        return skipCount;
    }

    public boolean isSkipImages() {
        return skipImages;
    }

    public void setSkipImages(boolean skipImages) {
        this.skipImages = skipImages;
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

                SolrInputDocument update = transformer.apply(content.getValue());
                if (update != null) {
                    return update;
                } else {
                    skipCount++;
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
