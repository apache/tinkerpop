package com.tinkerpop.gremlin.process.graph.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.filter.FilterStep;
import com.tinkerpop.gremlin.process.util.Reversible;
import com.tinkerpop.gremlin.process.util.UnBulkable;
import com.tinkerpop.gremlin.util.function.SConsumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SideEffectStep<S> extends FilterStep<S> implements Reversible, UnBulkable {

    public SideEffectStep(final Traversal traversal, final SConsumer<Traverser<S>> consumer) {
        super(traversal);
        this.setPredicate(traverser -> {
            consumer.accept(traverser);
            return true;
        });
    }

}
