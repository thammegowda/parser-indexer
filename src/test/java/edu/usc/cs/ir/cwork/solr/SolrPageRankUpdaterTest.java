package edu.usc.cs.ir.cwork.solr;

import edu.usc.cs.ir.cwork.Main;
import edu.usc.cs.ir.cwork.graph.Graph;
import edu.usc.cs.ir.cwork.graph.Vertex;
import edu.usc.cs.ir.cwork.relevance.PageRanker;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

public class SolrPageRankUpdaterTest {

    private String collectionName = "pageRankTest";
    private String instanceDir = "pageRankTest";
    private String serverURL = "http://127.0.0.1:8983/solr/";

    @Test
    public void TestPageRankUpdater() throws Exception{
//        SolrServer server =  new HttpSolrServer(serverURL);
//        try {
//            createTestCore(collectionName, instanceDir, server);
//        }catch (IOException e){
//            Main.LOG.debug(e.toString());
//        }
//
//        SolrPageRankUpdater updater = new SolrPageRankUpdater(createGraph(), serverURL+"pageRankTest");
//        updater.run();

    }

    public Graph createGraph(){
        Vertex v1 = new Vertex("v1", 0.0);
        Vertex v2 = new Vertex("v2", 0.0);
        Vertex v3 = new Vertex("v3", 0.0);
        Vertex v4 = new Vertex("v4", 0.0);

        v1.addUndirectedEdge(v2);
        v1.addUndirectedEdge(v3);
        v1.addUndirectedEdge(v4);
        v2.addUndirectedEdge(v3);

        Set<Vertex> vertices = new HashSet<>();
        vertices.add(v1);
        vertices.add(v2);
        vertices.add(v3);
        vertices.add(v4);

        Graph graph = new Graph("test_pr", vertices);

        PageRanker ranker = new PageRanker();
        ranker.rank(graph, 15, 0.5);
        return graph;
    }

    public void createTestCore(String name, String instanceDir, SolrServer server) throws IOException, SolrServerException{

        // TODO add code to create core and it's schema programmatically
        // I.e. create a core/collection named pageRankTest and add fields to schema

        CoreAdminResponse aResponse = CoreAdminRequest.getStatus(name, server);

        // If test core doesn't exist, create it
        if (aResponse.getCoreStatus().size() < 1)
        {
            CoreAdminResponse aNewInstance = new CoreAdminRequest().createCore(name, instanceDir, server);
        }

    }
}