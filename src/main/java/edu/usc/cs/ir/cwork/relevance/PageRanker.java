package edu.usc.cs.ir.cwork.relevance;

import edu.usc.cs.ir.cwork.graph.Graph;
import edu.usc.cs.ir.cwork.graph.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by tg on 11/4/15.
 */
public class PageRanker {

    private static final Logger LOG = LoggerFactory.getLogger(PageRanker.class);

    private boolean debug = true;

    public boolean isDebug() {
        return debug;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public void rank(Graph graph, int numIterations, double dampingFactor) {
        Set<Vertex> vertices = graph.getVertices();
        int n = vertices.size();

        double baseLine = (1 - dampingFactor)/n;

        long st = System.currentTimeMillis();
        int delay = 2 * 1000;
        for (int iteration = 0; iteration < numIterations; iteration++) {
            int count = 0;

            Map<String, Double> scores = new HashMap<>();
            for (Vertex thisVtx : vertices) {
                Double sum = thisVtx.getEdges().stream()
                        .map(v -> v.getScore() / v.getEdges().size())
                        .reduce(0.0, (f1, f2) -> f1 + f2);
                double pageRank = baseLine + dampingFactor * sum;

                //thisVtx.setScore(pageRank); //Updated at the end of iteration
                scores.put(thisVtx.getId(), pageRank);
                count++;
                if (System.currentTimeMillis() - st > delay) {
                    LOG.info("Iteration {} of {}, vertex {} of {}", iteration,
                            numIterations, count, n);
                    st = System.currentTimeMillis();
                }
            }
            vertices.forEach(v -> v.setScore(scores.get(v.getId())));
            if (debug) {
                System.out.println("\nIteration " + iteration + " complete.");
                printPageRanks(graph);
            }
        }
    }

    public void printPageRanks(Graph graph) {
        Set<Vertex> vertices = graph.getVertices();
        for (Vertex vertice : vertices) {
            System.out.println(vertice.getId() + " : " + vertice.getScore());
        }

    }
}
