package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.FunctionRingAcceptor;
import com.tinkerpop.gremlin.process.graph.marker.Reducing;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.util.FunctionRing;
import org.javatuples.Pair;

import java.util.HashSet;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class DedupStep<S> extends FilterStep<S> implements Reversible, Reducing, FunctionRingAcceptor<S, Object> {

    private Function<S, ?> uniqueFunction = null;
    private Set<Object> duplicateSet = new HashSet<>();

    public DedupStep(final Traversal traversal) {
        super(traversal);
        DedupStep.generatePredicate(this);
    }

    public boolean hasUniqueFunction() {
        return null == this.uniqueFunction;
    }

    @Override
    public void setFunctionRing(final FunctionRing<S, Object> functionRing) {
        FunctionRingAcceptor.singleFunctionSupported(functionRing, this);
        this.uniqueFunction = functionRing.next();
        DedupStep.generatePredicate(this);
    }

    @Override
    public Pair<Supplier<Set>, BiFunction<Set, Traverser<S>, Set>> getReducer() {
        return Pair.with(HashSet::new, (set, traverser) -> {
            set.add(null == this.uniqueFunction ? traverser.get() : this.uniqueFunction.apply(traverser.get()));
            return set;
        });
    }

    @Override
    public DedupStep<S> clone() throws CloneNotSupportedException {
        final DedupStep<S> clone = (DedupStep<S>) super.clone();
        clone.duplicateSet = new HashSet<>();
        generatePredicate(clone);
        return clone;
    }

    @Override
    public void reset() {
        super.reset();
        this.duplicateSet.clear();
    }

    /////////////////////////

    private static final <S> void generatePredicate(final DedupStep<S> dedupStep) {
        if (null == dedupStep.uniqueFunction) {
            dedupStep.setPredicate(traverser -> {
                traverser.asAdmin().setBulk(1);
                return dedupStep.duplicateSet.add(traverser.get());
            });
        } else {
            dedupStep.setPredicate(traverser -> {
                traverser.asAdmin().setBulk(1);
                return dedupStep.duplicateSet.add(dedupStep.uniqueFunction.apply(traverser.get()));
            });
        }
    }
}
