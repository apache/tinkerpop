package com.tinkerpop.gremlin.process.computer;

import com.tinkerpop.gremlin.structure.Graph;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ComputerResult implements Serializable {

    private final Graph graph;
    private final Memory memory;

    public ComputerResult(final Graph graph, final Memory memory) {
        this.graph = graph;
        this.memory = memory;
    }

    public Graph getGraph() {
        return this.graph;
    }

    public Memory getMemory() {
        return this.memory;
    }

    public String toString() {
        return "result[" + this.graph + "," + this.memory + "]";
    }
}
