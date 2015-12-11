package edu.usc.cs.ir.cwork.solr.schema;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * Created by tg on 10/28/15.
 */
public class StringEvaluatorTest {

    @Test
    public void testValuate() throws Exception {
        StringEvaluator valuator = new StringEvaluator();
        assertEquals(1234L, valuator.valueOf("1234"));
        long l = (long)Integer.MAX_VALUE + 1;
        assertEquals(l, valuator.valueOf(l + ""));

        assertEquals(true, valuator.valueOf("true"));
        assertEquals(true, valuator.valueOf("True"));
        assertEquals(true, valuator.valueOf("TRUE"));
        assertEquals(false, valuator.valueOf("False"));
        assertEquals(false, valuator.valueOf("false"));
        assertEquals(false, valuator.valueOf("FALSE"));
        assertEquals(0.0, valuator.valueOf("0.0"));
        assertEquals(+1.4, valuator.valueOf("+1.4"));
        assertEquals(-1.4, valuator.valueOf("-1.4"));
        assertEquals(2.0, valuator.valueOf("2.0"));
        assertEquals(2.2960336E-5, valuator.valueOf("2.2960336E-5"));

        //FIXME: Date
        //Date date = new Date();
        //assertEquals(date, valuator.valueOf(date.toString()));
    }

    @Test
    public void testCanEval() throws Exception {
        StringEvaluator valuator = new StringEvaluator();
        assertEquals(true, valuator.canEval("abcd"));
        assertEquals(true, valuator.canEval(new String[]{"1", "2"}));
        assertEquals(false, valuator.canEval(1234));
        assertEquals(false, valuator.canEval(new int[]{1, 2}));
    }

    @Test
    public void testEval() throws Exception {
        StringEvaluator valuator = new StringEvaluator();

        assertEquals(1L, valuator.eval("1"));
        Object result = valuator.eval(new String[]{"1", "2"});
        System.out.println(result);
        assertEquals(Arrays.asList(1L, 2L), result);
        assertEquals(1.1, valuator.eval("1.1"));
        assertEquals(true, valuator.eval("true"));
    }
}