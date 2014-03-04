package com.tinkerpop.gremlin.process.graph.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.filter.FilterStep;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CountStep<S> extends FilterStep<S> implements SideEffectCapable {

    private Long counter = 0l;

    public CountStep(final Traversal traversal) {
        super(traversal);
        this.traversal.memory().set(CAP_VARIABLE, this.counter);
        this.setPredicate(holder -> {
            this.counter++;
            return true;
        });
    }
}
