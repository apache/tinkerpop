package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.Bulkable;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.marker.VertexCentric;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce.CountCapMapReduce;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CountCapStep<S> extends FilterStep<S> implements SideEffectCapable, Bulkable, VertexCentric, MapReducer<MapReduce.NullObject, Long, MapReduce.NullObject, Long, Long> {

    private long bulkCount = 1l;
    private AtomicLong count = new AtomicLong(0l);
    private boolean vertexCentric = false;

    public CountCapStep(final Traversal traversal) {
        super(traversal);
        this.setPredicate(traverser -> {
            this.count.set(this.count.get() + this.bulkCount);
            if (!vertexCentric) this.traversal.memory().set(CAP_KEY, this.count.get());
            return true;
        });

    }

    public void setCurrentBulkCount(final long bulkCount) {
        this.bulkCount = bulkCount;
    }

    public void setCurrentVertex(final Vertex vertex) {
        this.vertexCentric = true;
        this.count = vertex.<AtomicLong>property(CAP_KEY).orElse(new AtomicLong(0));
        if (!vertex.property(CAP_KEY).isPresent())
            vertex.property(CAP_KEY, this.count);
    }

    public MapReduce<MapReduce.NullObject, Long, MapReduce.NullObject, Long, Long> getMapReduce() {
        return new CountCapMapReduce(this);
    }

    public String getVariable() {
        return CAP_KEY;
    }
}
