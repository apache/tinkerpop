package com.tinkerpop.gremlin.process.computer.traversal;

import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalCounters implements Serializable {

    protected Map<Traverser, Long> previousObjectTracks;
    protected final Map<Traverser, Long> graphTracks = new HashMap<>();
    protected final Map<Traverser, Long> objectTracks = new HashMap<>();

    protected final Map<Traverser, Long> doneGraphTracks = new HashMap<>();
    protected final Map<Traverser, Long> doneObjectTracks = new HashMap<>();

    public TraversalCounters() {
    }

    public TraversalCounters(final Vertex vertex) {
        final Property<TraversalCounters> tracker = vertex.property(TraversalVertexProgram.TRAVERSER_TRACKER);
        this.previousObjectTracks = tracker.isPresent() ? tracker.value().objectTracks : new HashMap<>();
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

    /*public static class TraversalCountersSerializer<T> extends Serializer<T> {
        @Override
        public void write(final Kryo kryo, final Output output, final T traversalCounters) {
            kryo.writeClassAndObject(output, traversalCounters);
        }

        @Override
        public T read(final Kryo kryo, final Input input, final Class<T> traversalCounters) {
            return (T) kryo.readClassAndObject(input);
        }
    }*/
}


