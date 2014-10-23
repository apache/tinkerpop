package com.tinkerpop.gremlin.process.computer.traversal;

import com.tinkerpop.gremlin.process.util.TraverserSet;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraverserTracker implements Serializable {

    protected final TraverserSet doneGraphTracks = new TraverserSet();
    protected final TraverserSet doneObjectTracks = new TraverserSet();

    public TraverserTracker() {
    }

    public TraverserSet getDoneGraphTracks() {
        return this.doneGraphTracks;
    }

    public TraverserSet getDoneObjectTracks() {
        return this.doneObjectTracks;
    }
}

