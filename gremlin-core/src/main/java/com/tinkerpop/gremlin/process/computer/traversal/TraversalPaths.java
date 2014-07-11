package com.tinkerpop.gremlin.process.computer.traversal;

import com.tinkerpop.gremlin.process.Traverser;
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

    protected Map<Object, List<Traverser>> previousObjectTracks;
    protected final Map<Object, List<Traverser>> graphTracks = new HashMap<>();
    protected final Map<Object, List<Traverser>> objectTracks = new HashMap<>();

    protected Map<Object, List<Traverser>> doneGraphTracks;
    protected Map<Object, List<Traverser>> doneObjectTracks;

    public TraversalPaths() {
    }

    public TraversalPaths(final Vertex vertex) {
        final Property<TraversalPaths> tracker = vertex.property(TraversalVertexProgram.TRAVERSER_TRACKER);
        this.previousObjectTracks = tracker.isPresent() ? tracker.value().objectTracks : new HashMap<>();
        this.doneGraphTracks = tracker.isPresent() ? tracker.value().doneGraphTracks : new HashMap<>();
        this.doneObjectTracks = tracker.isPresent() ? tracker.value().doneObjectTracks : new HashMap<>();
    }

    public Map<Object, List<Traverser>> getDoneGraphTracks() {
        return this.doneGraphTracks;
    }

    public Map<Object, List<Traverser>> getDoneObjectTracks() {
        return this.doneObjectTracks;
    }

    public Map<Object, List<Traverser>> getObjectTracks() {
        return this.objectTracks;
    }

    public Map<Object, List<Traverser>> getGraphTracks() {
        return this.graphTracks;
    }

    public Map<Object, List<Traverser>> getPreviousObjectTracks() {
        return this.previousObjectTracks;
    }
}
