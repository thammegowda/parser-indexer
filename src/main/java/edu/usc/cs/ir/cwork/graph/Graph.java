package edu.usc.cs.ir.cwork.graph;

import java.util.Set;

/**
 * Graph model
 */
public class Graph {

    private String type;
    private Set<Vertex> vertices;

    public Graph(String type, Set<Vertex> vertices) {
        this.type = type;
        this.vertices = vertices;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Set<Vertex> getVertices() {
        return vertices;
    }

    public void setVertices(Set<Vertex> vertices) {
        this.vertices = vertices;
    }
}
