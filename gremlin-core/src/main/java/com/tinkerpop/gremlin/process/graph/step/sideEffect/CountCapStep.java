package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Bulkable;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CountCapStep<S> extends FilterStep<S> implements Bulkable, SideEffectCapable {

    private long bulkCount = 1l;

    public CountCapStep(final Traversal traversal) {
        super(traversal);
        traversal.memory().getOrCreate(CAP_KEY, () -> 0l);
        this.setPredicate(traverser -> {
            synchronized (this) {
                final Long count = traversal.memory().getOrCreate(CAP_KEY, () -> 0l);
                traversal.memory().set(CAP_KEY, count + this.bulkCount);
                return true;
            }
        });
    }

    public void setCurrentBulkCount(final long bulkCount) {
        this.bulkCount = bulkCount;
    }
}
