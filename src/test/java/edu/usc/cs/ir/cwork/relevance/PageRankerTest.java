package edu.usc.cs.ir.cwork.relevance;

import edu.usc.cs.ir.cwork.graph.Graph;
import edu.usc.cs.ir.cwork.graph.Vertex;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Created by tg on 11/4/15.
 */
public class PageRankerTest {

    @Test
    public void testRank() throws Exception {


        Vertex v1 = new Vertex("v1", 0.0);
        Vertex v2 = new Vertex("v2", 0.0);
        Vertex v3 = new Vertex("v3", 0.0);
        Vertex v4 = new Vertex("v4", 0.0);

        v1.addUndirectedEdge(v2);
        v1.addUndirectedEdge(v3);
        v1.addUndirectedEdge(v4);

        Set<Vertex> vertices = new HashSet<>();
        vertices.add(v1);
        vertices.add(v2);
        vertices.add(v3);
        vertices.add(v4);

        Graph graph = new Graph("test", vertices);

        PageRanker ranker = new PageRanker();
        ranker.rank(graph, 15, 0.5);

    }
}