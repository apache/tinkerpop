package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.Reducing;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import org.javatuples.Pair;

import java.util.HashSet;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class DedupStep<S> extends FilterStep<S> implements Reversible, Reducing {

    private final Function<Traverser<S>, ?> uniqueFunction;

    public DedupStep(final Traversal traversal, final Function<Traverser<S>, ?> uniqueFunction) {
        super(traversal);
        this.uniqueFunction = uniqueFunction;
        final Set<Object> set = new HashSet<>();
        if (null == uniqueFunction) {
            this.setPredicate(traverser -> {
                traverser.asAdmin().setBulk(1);
                return set.add(traverser.get());
            });
        } else {
            this.setPredicate(traverser -> {
                traverser.asAdmin().setBulk(1);
                return set.add(this.uniqueFunction.apply(traverser));
            });
        }
    }

    public DedupStep(final Traversal traversal) {
        this(traversal, null);
    }

    public boolean hasUniqueFunction() {
        return null == this.uniqueFunction;
    }

    @Override
    public Pair<Supplier<Set>, BiFunction<Set, Traverser<S>, Set>> getReducer() {
        return Pair.with(HashSet::new, (set, traverser) -> {
            set.add(null == this.uniqueFunction ? traverser.get() : this.uniqueFunction.apply(traverser));
            return set;
        });
    }

    /*@Override
    public void reset() { // TODO: reset the hashset? .. but if this becomes a SideEffectStep?
        super.reset();
    }*/
}
