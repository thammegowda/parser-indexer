package edu.usc.cs.ir.cwork.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This is a decorator iterator which creates groups from stream.
 * This is useful for parallelizing using threads.
 */
public class GroupedIterator<T> implements Iterator<List<T>> {

    private Iterator<T> iterator;
    private int groupSize;

    private List<T> next;

    public GroupedIterator(Iterator<T> iterator, int groupSize) {
        this.iterator = iterator;
        this.groupSize = groupSize;
        this.next = getNext();
    }

    private List<T> getNext(){
        List<T> list = null;
        if (iterator != null && iterator.hasNext()) {
            int count = 0;
            list = new ArrayList<>(groupSize);
            while (count < groupSize && iterator.hasNext()) {
                list.add(iterator.next());
                count++;
            }
        }
        return list;
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public List<T> next() {
        List<T> tmp = this.next;
        this.next = getNext();
        return tmp;
    }
}
