package com.tinkerpop.gremlin.process.computer.traversal;

import com.tinkerpop.gremlin.process.Traverser;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraverserCountTracker implements Serializable {

    protected Map<Traverser.Admin, Long> previousObjectTracks = new HashMap<>();
    protected final Map<Traverser.Admin, Long> objectTracks = new HashMap<>();

    protected final Map<Traverser.Admin, Long> doneGraphTracks = new HashMap<>();
    protected final Map<Traverser.Admin, Long> doneObjectTracks = new HashMap<>();

    public TraverserCountTracker() {
    }

    public Map<Traverser.Admin, Long> getDoneGraphTracks() {
        return this.doneGraphTracks;
    }

    public Map<Traverser.Admin, Long> getDoneObjectTracks() {
        return this.doneObjectTracks;
    }

    public Map<Traverser.Admin, Long> getObjectTracks() {
        return this.objectTracks;
    }

    public Map<Traverser.Admin, Long> getPreviousObjectTracks() {
        return this.previousObjectTracks;
    }

    public void completeIteration() {
        this.previousObjectTracks.clear();
        this.previousObjectTracks.putAll(this.objectTracks);
        this.objectTracks.clear();
    }
}

