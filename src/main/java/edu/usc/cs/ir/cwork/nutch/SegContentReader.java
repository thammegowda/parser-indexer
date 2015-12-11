package edu.usc.cs.ir.cwork.nutch;

import edu.usc.cs.ir.cwork.Context;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by tg on 10/25/15.
 */
public class SegContentReader {

    private static final Logger LOG = LoggerFactory.getLogger(SegContentReader.class);
    private List<String> paths;
    private FileSystem fs;

    public SegContentReader(List<String> paths) throws IOException {
        this.paths = paths;
        this.fs = FileSystem.get(Context.getInstance().getConf());
    }

    public List<Path> findAllParts(Path path) throws IOException {
        String name = path.getName();
        List<Path> parts = new ArrayList<>();
        String fileName = "data";
        if (fileName.equals(name)) {
            parts.add(path);
        } else if (fs.isDirectory(path)){
            RemoteIterator<LocatedFileStatus> files = fs.listFiles(path, true);
            while (files.hasNext()){
                LocatedFileStatus next = files.next();
                if (next.isFile() && fileName.equals(next.getPath().getName())) {
                    parts.add(next.getPath());
                }
            }
        }
        return parts;
    }

    public RecordIterator read() throws IOException {

        List<Path> partPaths = new ArrayList<>();
        for (String pathStr : paths) {
            pathStr = pathStr.trim();
            if (pathStr.isEmpty()) {
                LOG.warn("Skip : Empty line");
                continue;
            }
            Path path = new Path(pathStr);
            partPaths.addAll(findAllParts(path));
        }
        return new RecordIterator(partPaths);
    }
}
