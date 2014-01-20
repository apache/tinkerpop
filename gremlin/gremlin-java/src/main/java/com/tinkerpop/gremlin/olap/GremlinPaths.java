package com.tinkerpop.gremlin.olap;

import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.Holder;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinPaths implements Serializable {

    protected final Map<Object, List<Holder>> previousObjectTracks;
    protected final Map<Object, List<Holder>> graphTracks = new HashMap<>();
    protected final Map<Object, List<Holder>> objectTracks = new HashMap<>();

    protected final Map<Object, List<Holder>> doneGraphTracks = new HashMap<>();
    protected final Map<Object, List<Holder>> doneObjectTracks = new HashMap<>();

    public GremlinPaths(final Vertex vertex) {
        final Property<GremlinPaths> tracker = vertex.getProperty(GremlinVertexProgram.GREMLIN_TRACKER);
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
