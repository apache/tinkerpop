package com.tinkerpop.gremlin.process.computer.traversal;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalCounters implements Serializable {

    protected final Map<Holder, Long> previousObjectTracks;
    protected final Map<Holder, Long> graphTracks = new HashMap<>();
    protected final Map<Holder, Long> objectTracks = new HashMap<>();

    protected final Map<Holder, Long> doneGraphTracks = new HashMap<>();
    protected final Map<Holder, Long> doneObjectTracks = new HashMap<>();

    public TraversalCounters(final Vertex vertex) {
        final Property<TraversalCounters> tracker = vertex.property(TraversalVertexProgram.TRAVERSAL_TRACKER);
        this.previousObjectTracks = tracker.isPresent() ? tracker.value().objectTracks : new HashMap<>();
    }

    public Map<Holder, Long> getDoneGraphTracks() {
        return this.doneGraphTracks;
    }

    public Map<Holder, Long> getDoneObjectTracks() {
        return this.doneObjectTracks;
    }

    public Map<Holder, Long> getObjectTracks() {
        return this.objectTracks;
    }

    public Map<Holder, Long> getGraphTracks() {
        return this.graphTracks;
    }

    public Map<Holder, Long> getPreviousObjectTracks() {
        return this.previousObjectTracks;
    }
}


