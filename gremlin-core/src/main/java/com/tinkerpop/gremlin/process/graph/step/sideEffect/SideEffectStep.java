package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.BulkSet;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Consumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SideEffectStep<S> extends AbstractStep<S, S> implements Reversible {

    private Consumer<Traverser<S>> consumer = null;

    public SideEffectStep(final Traversal traversal) {
        super(traversal);
    }

    public void setConsumer(final Consumer<Traverser<S>> consumer) {
        this.consumer = consumer;
    }

    @Override
    protected Traverser<S> processNextStart() {
        final Traverser<S> traverser = this.starts.next();
        if (null != this.consumer) this.consumer.accept(traverser);
        return traverser;
    }

    public static <S> void addToCollection(final Collection<S> collection, final S s, final long bulk) {
        if (collection instanceof BulkSet) {
            ((BulkSet<S>) collection).add(s, bulk);
        } else if (collection instanceof Set) {
            collection.add(s);
        } else {
            for (long i = 0; i < bulk; i++) {
                collection.add(s);
            }
        }
    }

    public static <S> void addToCollectionUnrollIterator(final Collection<S> collection, final S s, final long bulk) {
        if (s instanceof Iterator) {
            ((Iterator<S>) s).forEachRemaining(r -> SideEffectStep.addToCollection(collection, r, bulk));
        } else if (s instanceof Iterable) {
            ((Iterable<S>) s).forEach(r -> SideEffectStep.addToCollection(collection, r, bulk));
        } else {
            SideEffectStep.addToCollection(collection, s, bulk);
        }
    }
}
