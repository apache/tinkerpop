package com.tinkerpop.gremlin.computer.gremlin;

import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.pipes.util.Holder;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinCounters implements Serializable {

    protected final Map<Holder, Long> previousObjectTracks;
    protected final Map<Holder, Long> graphTracks = new HashMap<>();
    protected final Map<Holder, Long> objectTracks = new HashMap<>();

    protected final Map<Holder, Long> doneGraphTracks = new HashMap<>();
    protected final Map<Holder, Long> doneObjectTracks = new HashMap<>();

    public GremlinCounters(final Vertex vertex) {
        final Property<GremlinCounters> tracker = vertex.getProperty(GremlinVertexProgram.GREMLIN_TRACKER);
        this.previousObjectTracks = tracker.isPresent() ? tracker.get().objectTracks : new HashMap<>();
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


