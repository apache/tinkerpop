package com.tinkerpop.gremlin.process.computer.traversal;

import com.tinkerpop.gremlin.process.Traverser;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraverserPathTracker implements Serializable {

    protected final Map<Object, List<Traverser.System>> previousObjectTracks = new HashMap<>();
    protected final Map<Object, List<Traverser.System>> graphTracks = new HashMap<>();
    protected final Map<Object, List<Traverser.System>> objectTracks = new HashMap<>();

    protected final Map<Object, List<Traverser.System>> doneGraphTracks = new HashMap<>();
    protected final Map<Object, List<Traverser.System>> doneObjectTracks = new HashMap<>();

    public TraverserPathTracker() {
    }

    public Map<Object, List<Traverser.System>> getDoneGraphTracks() {
        return this.doneGraphTracks;
    }

    public Map<Object, List<Traverser.System>> getDoneObjectTracks() {
        return this.doneObjectTracks;
    }

    public Map<Object, List<Traverser.System>> getObjectTracks() {
        return this.objectTracks;
    }

    public Map<Object, List<Traverser.System>> getGraphTracks() {
        return this.graphTracks;
    }

    public Map<Object, List<Traverser.System>> getPreviousObjectTracks() {
        return this.previousObjectTracks;
    }

    public void completeIteration() {
        this.previousObjectTracks.clear();
        this.previousObjectTracks.putAll(this.objectTracks);
        this.objectTracks.clear();
        this.graphTracks.clear();
    }
}
