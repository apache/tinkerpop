package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.Bulkable;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.marker.VertexCentric;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce.CountCapMapReduce;
import com.tinkerpop.gremlin.structure.Graph;
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
            if (!this.vertexCentric) this.traversal.memory().set(this.getAs(), this.count.get());
            return true;
        });

    }

    public void setCurrentBulkCount(final long bulkCount) {
        this.bulkCount = bulkCount;
    }

    public void setCurrentVertex(final Vertex vertex) {
        this.vertexCentric = true;
        final String hiddenAs = Graph.Key.hide(this.getAs());
        this.count = vertex.<AtomicLong>property(hiddenAs).orElse(new AtomicLong(0));
        if (!vertex.property(hiddenAs).isPresent())
            vertex.property(hiddenAs, this.count);
    }

    public MapReduce<MapReduce.NullObject, Long, MapReduce.NullObject, Long, Long> getMapReduce() {
        return new CountCapMapReduce(this);
    }

}
