package edu.usc.cs.ir.cwork.graph;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by tg on 11/4/15.
 */
public class GraphTest {

    @Test
    public void testGraph() throws Exception {
        Vertex v1 = new Vertex("v1", 1.0);
        Vertex v2 = new Vertex("v1", 2.0);
        Vertex v3 = new Vertex("v1", 3.0);

        v1.addUndirectedEdge(v2);

    }
}