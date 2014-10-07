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

    protected final Map<Object, List<Traverser.Admin>> previousObjectTracks = new HashMap<>();
    protected final Map<Object, List<Traverser.Admin>> objectTracks = new HashMap<>();

    protected final Map<Object, List<Traverser.Admin>> doneGraphTracks = new HashMap<>();
    protected final Map<Object, List<Traverser.Admin>> doneObjectTracks = new HashMap<>();

    public TraverserPathTracker() {
    }

    public Map<Object, List<Traverser.Admin>> getDoneGraphTracks() {
        return this.doneGraphTracks;
    }

    public Map<Object, List<Traverser.Admin>> getDoneObjectTracks() {
        return this.doneObjectTracks;
    }

    public Map<Object, List<Traverser.Admin>> getObjectTracks() {
        return this.objectTracks;
    }

    public Map<Object, List<Traverser.Admin>> getPreviousObjectTracks() {
        return this.previousObjectTracks;
    }

    public void completeIteration() {
        this.previousObjectTracks.clear();
        this.previousObjectTracks.putAll(this.objectTracks);
        this.objectTracks.clear();
    }
}