package com.tinkerpop.gremlin.tinkergraph.process.computer;

import com.tinkerpop.gremlin.structure.Vertex;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class TinkerMessageBoard<M> {

    public Map<Vertex, Queue<M>> sendMessages = new HashMap<>();
    public Map<Vertex, Queue<M>> receiveMessages = new HashMap<>();

    public void completeIteration() {
        this.receiveMessages = this.sendMessages;
        this.sendMessages = new HashMap<>();
    }
}
