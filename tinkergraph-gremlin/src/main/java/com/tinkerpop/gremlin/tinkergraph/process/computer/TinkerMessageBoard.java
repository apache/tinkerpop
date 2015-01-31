package com.tinkerpop.gremlin.tinkergraph.process.computer;

import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class TinkerMessageBoard<M> {

    public Map<Vertex, Queue<M>> sendMessages = new ConcurrentHashMap<>();
    public Map<Vertex, Queue<M>> receiveMessages = new ConcurrentHashMap<>();

    public void completeIteration() {
        this.receiveMessages = this.sendMessages;
        this.sendMessages = new ConcurrentHashMap<>();
    }
}
