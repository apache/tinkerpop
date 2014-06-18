package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.util.function.SConsumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SideEffectStep<S> extends FilterStep<S> implements Reversible {

    public SideEffectStep(final Traversal traversal, final SConsumer<Traverser<S>> consumer) {
        super(traversal);
        this.setPredicate(traverser -> {
            consumer.accept(traverser);
            return true;
        });
    }

}
