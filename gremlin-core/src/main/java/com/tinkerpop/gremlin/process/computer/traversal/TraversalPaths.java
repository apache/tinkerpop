package com.tinkerpop.gremlin.process.computer.traversal;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalPaths implements Serializable {

    protected final Map<Object, List<Holder>> previousObjectTracks;
    protected final Map<Object, List<Holder>> graphTracks = new HashMap<>();
    protected final Map<Object, List<Holder>> objectTracks = new HashMap<>();

    protected final Map<Object, List<Holder>> doneGraphTracks = new HashMap<>();
    protected final Map<Object, List<Holder>> doneObjectTracks = new HashMap<>();

    public TraversalPaths(final Vertex vertex) {
        final Property<TraversalPaths> tracker = vertex.property(TraversalVertexProgram.TRAVERSAL_TRACKER);
        this.previousObjectTracks = tracker.isPresent() ? tracker.get().objectTracks : new HashMap<>();
    }

    public Map<Object, List<Holder>> getDoneGraphTracks() {
        return this.doneGraphTracks;
    }

    public Map<Object, List<Holder>> getDoneObjectTracks() {
        return this.doneObjectTracks;
    }

    public Map<Object, List<Holder>> getObjectTracks() {
        return this.objectTracks;
    }

    public Map<Object, List<Holder>> getGraphTracks() {
        return this.graphTracks;
    }

    public Map<Object, List<Holder>> getPreviousObjectTracks() {
        return this.previousObjectTracks;
    }
}
