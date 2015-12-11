package edu.usc.cs.ir.cwork.tika;

import edu.usc.cs.ir.cwork.solr.Phase2Indexer;
import org.junit.Test;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Created by tg on 11/9/15.
 */
public class ParserTest {

    @Test
    public void testCleanDates() throws Exception {
        Set<Date> dates = new HashSet<>();
        dates.add(new Date());
        dates.add(new Date(System.currentTimeMillis() - 23 * 60 * 60 * 1000));
        dates.add(new Date(System.currentTimeMillis() - 25 * 60 * 60 * 1000));
        Set<Date> cleaned = Parser.filterDates(dates);
        assertTrue(cleaned.size() == 1);
    }

}