package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Bulkable;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CountStep<S> extends FilterStep<S> implements SideEffectCapable, Reversible, Bulkable {

    private long counter = 0l;
    private long bulkCount = 1l;

    public CountStep(final Traversal traversal) {
        super(traversal);
        this.traversal.memory().set(CAP_KEY, this.counter);
        this.setPredicate(traverser -> {
            this.counter = this.counter + this.bulkCount;
            return true;
        });
    }

    public void setCurrentBulkCount(final long bulkCount) {
        this.bulkCount = bulkCount;
    }
}
