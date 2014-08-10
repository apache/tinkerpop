package com.tinkerpop.gremlin.tinkergraph.process.computer;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class TinkerMessageBoard<M> {

    // Map<VertexId, MessageQueue>
    public Map<Object, Queue<M>> sendMessages = new HashMap<>();
    public Map<Object, Queue<M>> receiveMessages = new HashMap<>();

    public void completeIteration() {
        this.receiveMessages = this.sendMessages;
        this.sendMessages = new HashMap<>();
    }
}
