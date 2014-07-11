package com.tinkerpop.gremlin.process.computer.traversal;

import com.tinkerpop.gremlin.process.Traverser;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraverserCountTracker implements Serializable {

    protected Map<Traverser, Long> previousObjectTracks = new HashMap<>();
    protected final Map<Traverser, Long> graphTracks = new HashMap<>();
    protected final Map<Traverser, Long> objectTracks = new HashMap<>();

    protected final Map<Traverser, Long> doneGraphTracks = new HashMap<>();
    protected final Map<Traverser, Long> doneObjectTracks = new HashMap<>();

    public TraverserCountTracker() {
    }

    public Map<Traverser, Long> getDoneGraphTracks() {
        return this.doneGraphTracks;
    }

    public Map<Traverser, Long> getDoneObjectTracks() {
        return this.doneObjectTracks;
    }

    public Map<Traverser, Long> getObjectTracks() {
        return this.objectTracks;
    }

    public Map<Traverser, Long> getGraphTracks() {
        return this.graphTracks;
    }

    public Map<Traverser, Long> getPreviousObjectTracks() {
        return this.previousObjectTracks;
    }

    public void completeIteration() {
        this.previousObjectTracks.clear();
        this.previousObjectTracks.putAll(this.objectTracks);
        this.objectTracks.clear();
        this.graphTracks.clear();
    }

}


