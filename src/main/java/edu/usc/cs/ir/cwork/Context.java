package edu.usc.cs.ir.cwork;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

/**
 * Created by tg on 10/25/15.
 */
public enum Context {
    INSTANCE;


    private Configuration conf;
    private FileSystem fs;

    Context() {
        conf = new Configuration();
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Context getInstance() {
        return INSTANCE;
    }

    public Configuration getConf() {
        return conf;
    }

    public FileSystem getFs() {
        return fs;
    }
}
