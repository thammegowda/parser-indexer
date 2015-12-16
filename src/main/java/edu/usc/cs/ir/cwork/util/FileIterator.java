package edu.usc.cs.ir.cwork.util;

import org.apache.commons.io.filefilter.IOFileFilter;

import java.io.File;
import java.util.Collections;
import java.util.Iterator;
import java.util.Stack;

/**
 * Iterates over the file system tree to find all the regular files.
 * You may need to use this when you are dealing with large number of files
 * because this doesn't create a list in memory.
 * It traverses the file tree as and when it is consumed.
 *
 * @see org.apache.commons.io.FileUtils#listFiles(File, IOFileFilter, IOFileFilter)
 * @see org.apache.commons.io.FileUtils#iterateFiles(File, IOFileFilter, IOFileFilter)
 */
public class FileIterator implements Iterator<File> {

    private File next;
    private final Stack<File> stack;

    private int numFiles;
    private int numDirs;

    /**
     * @param root the parent directory
     */
    public FileIterator(File root){
        this.stack = new Stack<>();
        this.stack.add(root);
        this.next = getNext();
    }


    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public File next() {
        try {
            return next;
        } finally {
            next = getNext();
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove not supported");
    }

    private File getNext(){
        File[] files;
        while (!stack.isEmpty()) {
            File top = stack.pop();
            if (top == null || !top.exists() || !top.canRead()) {
                continue;
            }
            if (top.isFile()) {
                numFiles++;
                return top;
            } else if ((files = top.listFiles()) != null) {
                numDirs++;
                Collections.addAll(stack, files);
            }
        }
        return null;
    }

    /**
     * Gets number of files visited so far
     * @return number of files seen
     */
    public int getNumFiles() {
        return numFiles;
    }

    /**
     * Gets number of directories visited so far
     * @return number of directories seen
     */
    public int getNumDirs() {
        return numDirs;
    }
}
