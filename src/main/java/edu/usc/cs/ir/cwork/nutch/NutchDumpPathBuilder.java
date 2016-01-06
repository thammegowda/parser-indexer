package edu.usc.cs.ir.cwork.nutch;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.nutch.util.TableUtil;

import java.io.File;
import java.net.URL;
import java.util.function.Function;

/**
 *
 * Maps url to path string (same format as nutch file dumper)
 * Created by tg on 1/5/16.
 */
public class NutchDumpPathBuilder implements Function<URL, String> {

    private final String dumpDir;

    public NutchDumpPathBuilder(String dumpDir) {
        this.dumpDir = dumpDir;
    }

    @Override
    public String apply(URL url) {

        String[] reversedURL = TableUtil.reverseUrl(url).split(":");
        reversedURL[0] = reversedURL[0].replace('.', '/');

        String reversedURLPath = reversedURL[0] + "/" + DigestUtils.sha256Hex(url.toString()).toUpperCase();
        String pathStr = String.format("%s/%s", dumpDir, reversedURLPath);
        return new File(pathStr).toURI().toString();
    }
}
