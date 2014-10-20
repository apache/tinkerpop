package com.tinkerpop.gremlin.process.computer.traversal;

import com.tinkerpop.gremlin.process.util.TraverserSet;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraverserTracker implements Serializable {

    protected final TraverserSet<?> previousObjectTracks = new TraverserSet<>();
    protected final TraverserSet<?> objectTracks = new TraverserSet<>();

    protected final TraverserSet<?> doneGraphTracks = new TraverserSet<>();
    protected final TraverserSet<?> doneObjectTracks = new TraverserSet<>();

    public TraverserTracker() {
    }

    public TraverserSet<?> getDoneGraphTracks() {
        return this.doneGraphTracks;
    }

    public TraverserSet<?> getDoneObjectTracks() {
        return this.doneObjectTracks;
    }

    public TraverserSet<?> getObjectTracks() {
        return this.objectTracks;
    }

    public TraverserSet<?> getPreviousObjectTracks() {
        return this.previousObjectTracks;
    }

    public void completeIteration() {
        this.previousObjectTracks.clear();
        this.previousObjectTracks.addAll((TraverserSet) this.objectTracks);
        this.objectTracks.clear();
    }
}

