package com.tinkerpop.gremlin.computer.gremlin;

import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.pipes.util.Holder;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinTracker implements Serializable {

    protected final Map<Object, List<Holder>> previousObjectHolders;
    protected final Map<Object, List<Holder>> graphHolders = new HashMap<>();
    protected final Map<Object, List<Holder>> objectHolders = new HashMap<>();

    protected final Map<Object, List<Holder>> doneGraphHolders = new HashMap<>();
    protected final Map<Object, List<Holder>> doneObjectHolders = new HashMap<>();

    public GremlinTracker(final Vertex vertex) {
        final Property<GremlinTracker> tracker = vertex.getProperty(GremlinVertexProgram.GREMLIN_TRACKER);
        this.previousObjectHolders = tracker.isPresent() ? tracker.get().objectHolders : new HashMap<>();
    }

    public Map<Object, List<Holder>> getDoneGraphHolders() {
        return this.doneGraphHolders;
    }

    public Map<Object, List<Holder>> getDoneObjectHolders() {
        return this.doneObjectHolders;
    }

    public Map<Object, List<Holder>> getObjectHolders() {
        return this.objectHolders;
    }

    public Map<Object, List<Holder>> getGraphHolders() {
        return this.graphHolders;
    }

    public Map<Object, List<Holder>> getPreviousObjectHolders() {
        return this.previousObjectHolders;
    }


}
