package edu.usc.cs.ir.cwork.tika;

import com.esotericsoftware.minlog.Log;
import com.joestelmach.natty.DateGroup;
import edu.usc.cs.ir.tika.ner.corenlp.CoreNLPNERecogniser;
import org.apache.commons.io.IOUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.protocol.Content;
import org.apache.tika.Tika;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ner.NamedEntityParser;
import org.apache.tika.parser.ner.regex.RegexNERecogniser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by tg on 10/25/15.
 */
public class Parser {

    public static final int A_DAY_MILLIS = 24 * 60 * 60 * 1000;
    public final Logger LOG = LoggerFactory.getLogger(Parser.class);
    public static final String PHASE1_CONF = "tika-config-phase1.xml";
    public static final String PHASE2_CONF = "tika-config-phase2.xml";
    private static final com.joestelmach.natty.Parser NATTY_PARSER =
            new com.joestelmach.natty.Parser();
    private static Parser PHASE1;

    private static Parser PHASE2;
    private Tika tika;

    public Parser(InputStream configStream) {
        try {
            TikaConfig config = new TikaConfig(configStream);
            tika = new Tika(config);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized static Parser getPhase1Parser(){
        if (PHASE1 == null) {
            PHASE1 = new Parser(Parser.class.getClassLoader()
                    .getResourceAsStream(PHASE1_CONF));
        }
        return PHASE1;
    }

    public  static Parser getPhase2Parser(){
        if (PHASE2 == null) {
            synchronized (Parser.class) {
                if (PHASE2 == null) {
                    String nerImpls = CoreNLPNERecogniser.class.getName()
                            + "," + RegexNERecogniser.class.getName();
                    System.setProperty(NamedEntityParser.SYS_PROP_NER_IMPL, nerImpls);
                    PHASE2 = new Parser(Parser.class.getClassLoader()
                            .getResourceAsStream(PHASE2_CONF));
                }
            }
        }
        return PHASE2;
    }

    /**
     * Parses the content
     * @param content  the nutch content to be parsed
     * @return the text content
     */
    public String parseContent(Content content){
        Pair<String, Metadata> pair = parse(content);
        return pair != null ? pair.getKey() : null;
    }

    /**
     * Parses the text content
     * @param content  the nutch content to be parsed
     * @return the text content
     */
    public Metadata parseContent(String content){
        try (InputStream stream = new ByteArrayInputStream(
                content.getBytes(StandardCharsets.UTF_8))){
            Pair<String, Metadata> result = parse(stream);
            return result == null ? null : result.getSecond();
        } catch (IOException e) {
            LOG.warn(e.getMessage(), e);
            return null;
        }
    }

    /**
     * Parses Nutch content to read text content and metadata
     * @param content nutch content
     * @return pair of text and metadata
     */
    public Pair<String, Metadata> parse(Content content){
        ByteArrayInputStream stream = new ByteArrayInputStream(content.getContent());
        try {
            return parse(stream);
        } finally {
            IOUtils.closeQuietly(stream);
        }
    }

    /**
     * Parses the stream to read text content and metadata
     * @param stream the stream
     * @return pair of text content and metadata
     */
    private Pair<String, Metadata> parse(InputStream stream) {
        Metadata metadata = new Metadata();
        try {
            String text = tika.parseToString(stream, metadata);
            return new Pair<>(text, metadata);
        } catch (IOException | TikaException e) {
            LOG.warn(e.getMessage(), e);
        }
        //something bad happened
        return null;
    }


    /**
     * Parses the URL content
     * @param url
     * @return
     * @throws IOException
     */
    public Pair<String, Metadata> parse(URL url) throws IOException, TikaException {
        Metadata metadata = new Metadata();
        try (InputStream stream = url.openStream()) {
            return new Pair<>(tika.parseToString(stream, metadata), metadata);
        }
    }


    /**
     * Filters dates that are within 24 hour time from now.
     * This is useful when date parser relate relative times to now
     * @param dates dates from which todays date time needs to be removed
     * @return filtered dates
     */
    public static Set<Date> filterDates(Set<Date> dates) {
        Set<Date> result = new HashSet<>();
        if (dates != null) {
            for (Date next : dates) {
                long freshness = Math.abs(System.currentTimeMillis() - next.getTime());
                if (freshness > A_DAY_MILLIS) {
                    result.add(next);
                }
            }
        }
        return result;
    }


    public static Set<Date> parseDates(String...values) {
        Set<Date> result = new HashSet<>();
        for (String value : values) {
            if (value == null) {
                continue;
            }
            List<DateGroup> groups = null;
            synchronized (NATTY_PARSER) {
                try {
                    groups = NATTY_PARSER.parse(value);
                } catch (Exception e) {
                    Log.debug(e.getMessage());
                }
            }
            if (groups != null) {
                for (DateGroup group : groups) {
                    List<Date> dates = group.getDates();
                    if (dates != null) {
                        result.addAll(dates);
                    }
                }
            }
        }
        return filterDates(result);
    }

    public static void main(String[] args) {

        Set<Date> dates = parseDates("August 1st 2015", "February",
                "February 2015", "15th february 2016");
        System.out.println(dates);
    }
}
