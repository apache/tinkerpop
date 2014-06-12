package com.tinkerpop.gremlin.process.graph.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.filter.FilterStep;
import com.tinkerpop.gremlin.process.util.Reversible;
import com.tinkerpop.gremlin.process.util.UnBulkable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CountStep<S> extends FilterStep<S> implements SideEffectCapable, Reversible, UnBulkable {

    private Long counter = 0l;

    public CountStep(final Traversal traversal) {
        super(traversal);
        this.traversal.memory().set(CAP_VARIABLE, this.counter);
        this.setPredicate(traverser -> {
            this.counter++;
            return true;
        });
    }
}
