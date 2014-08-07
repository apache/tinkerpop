package com.tinkerpop.gremlin.process.computer;

import com.tinkerpop.gremlin.structure.Graph;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ComputerResult implements Serializable {

    private final Graph graph;
    private final SideEffects sideEffects;

    public ComputerResult(final Graph graph, final SideEffects sideEffects) {
        this.graph = graph;
        this.sideEffects = sideEffects;
    }

    public Graph getGraph() {
        return this.graph;
    }

    public SideEffects getSideEffects() {
        return this.sideEffects;
    }

    public String toString() {
        return "result[" + this.graph + "," + this.sideEffects + "]";
    }
}
