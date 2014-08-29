package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.util.function.SBiConsumer;
import com.tinkerpop.gremlin.util.function.SConsumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SideEffectStep<S> extends AbstractStep<S, S> implements Reversible {

    public static final SConsumer NO_OP_CONSUMER = null;
    public static final SBiConsumer NO_OP_BI_CONSUMER = null;

    public SConsumer<Traverser<S>> consumer = NO_OP_CONSUMER;
    public SBiConsumer<Traverser<S>, Traversal.SideEffects> biConsumer = NO_OP_BI_CONSUMER;

    public SideEffectStep(final Traversal traversal) {
        super(traversal);
    }

    public void setConsumer(final SConsumer<Traverser<S>> consumer) {
        this.consumer = consumer;
    }

    public void setBiConsumer(final SBiConsumer<Traverser<S>, Traversal.SideEffects> biConsumer) {
        this.biConsumer = biConsumer;
    }

    @Override
    protected Traverser<S> processNextStart() {
        final Traverser<S> traverser = this.starts.next();
        if (NO_OP_CONSUMER != this.consumer) this.consumer.accept(traverser);
        if (NO_OP_BI_CONSUMER != this.biConsumer) this.biConsumer.accept(traverser, this.traversal.sideEffects());
        return traverser;
    }
}
