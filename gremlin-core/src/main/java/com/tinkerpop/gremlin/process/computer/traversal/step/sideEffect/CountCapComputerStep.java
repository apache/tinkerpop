package com.tinkerpop.gremlin.process.computer.traversal.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.Bulkable;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.VertexCentric;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.CountCapStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapable;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CountCapComputerStep<S> extends FilterStep<S> implements SideEffectCapable, Bulkable, VertexCentric, MapReducer {

    private long bulkCount = 1l;
    private AtomicLong count = new AtomicLong(0);

    public CountCapComputerStep(final Traversal traversal, final CountCapStep countStep) {
        super(traversal);
        this.setPredicate(traverser -> {
            this.count.set(this.count.get() + this.bulkCount);
            return true;
        });
        if (TraversalHelper.isLabeled(countStep))
            this.setAs(countStep.getAs());
    }

    public void setCurrentBulkCount(final long bulkCount) {
        this.bulkCount = bulkCount;
    }

    public void setCurrentVertex(final Vertex vertex) {
        this.count = vertex.<AtomicLong>property(CAP_KEY).orElse(new AtomicLong(0));
        if (!vertex.property(CAP_KEY).isPresent())
            vertex.property(CAP_KEY, this.count);
    }

    public MapReduce getMapReduce() {
        return new CountCapComputerMapReduce(this);
    }

    public String getVariable() {
        return CAP_KEY;
    }
}
