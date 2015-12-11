package edu.usc.cs.ir.cwork.tika;

import org.apache.commons.math3.util.Pair;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;

/**
 * Created by tg on 11/4/15.
 */
public class ParseChecker {

    public static void main(String[] args) throws IOException, TikaException {
        ///args = new String[]{"http://www.slickguns.com/product/tn-arms-2-receiver-bundle-free-shipping-magpul-colors-use-code-redd72015-pricing-8075?view=tiles"};
        if (args.length != 1) {
            System.out.println("Usage : <URL>");
            return;
        }

        String urlStr = args[0];
        URL url = new URL(urlStr);
        Pair<String, Metadata> pair = Parser.getPhase2Parser().parse(url);
        System.out.println("Content = \n" + pair.getFirst());
        Metadata metadata = pair.getSecond();
        for (String name : metadata.names()) {
            String[] vals = metadata.getValues(name);
            System.out.println(name + " : " + Arrays.toString(vals));
            if ("NER_DATE".equals(name)) {
                System.out.println("NER_DATES = "+ Parser.parseDates(vals));
            }
        }
    }
}
