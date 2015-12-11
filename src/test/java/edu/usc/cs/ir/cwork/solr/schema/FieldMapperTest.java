package edu.usc.cs.ir.cwork.solr.schema;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import static org.junit.Assert.*;

/**
 * Created by tg on 10/28/15.
 */
public class FieldMapperTest {

    @Test
    public void testMapField() throws Exception {
        FieldMapper mapper = FieldMapper.create();
        //test override
        assertEquals("id", mapper.mapField("id", "xxxx"));
        //test types
        assertEquals("a_t", mapper.mapField("a", "xxxx"));
        assertEquals("b_i", mapper.mapField("b", 10));
        assertEquals("b_i", mapper.mapField("b", new Integer(10)));
        assertEquals("c_l", mapper.mapField("c", 10l));
        assertEquals("c_l", mapper.mapField("c", new Long(10)));
        assertEquals("d_b", mapper.mapField("d", true));
        assertEquals("d_b", mapper.mapField("d", new Boolean(true)));
        assertEquals("e_f", mapper.mapField("e", 10.1f));
        assertEquals("e_f", mapper.mapField("e", new Float(10.1f)));
        assertEquals("f_d", mapper.mapField("f", 10.1));
        assertEquals("f_d", mapper.mapField("f", new Double(10.1)));

        assertEquals("a_ts", mapper.mapField("a", new String[]{"a","b", "c"}));
        assertEquals("a_ts", mapper.mapField("a", Arrays.asList("a","b", "c")));

        assertEquals("b_is", mapper.mapField("b", new int[]{10, 20}));
        assertEquals("b_is", mapper.mapField("b", new HashSet<>(Arrays.asList(10, 20))));
    }
}