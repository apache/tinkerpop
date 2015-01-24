package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.FunctionHolder;
import com.tinkerpop.gremlin.process.graph.marker.Reducing;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.process.util.TraversalLambda;
import com.tinkerpop.gremlin.process.util.TraversalObjectLambda;
import com.tinkerpop.gremlin.util.function.CloneableLambda;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class DedupStep<S> extends FilterStep<S> implements Reversible, Reducing<Set<Object>, S>, FunctionHolder<S, Object> {

    private static final Set<TraverserRequirement> REQUIREMENTS = new HashSet<>(Arrays.asList(
            TraverserRequirement.BULK,
            TraverserRequirement.OBJECT
    ));

    private Function<S, Object> uniqueFunction = null;
    private Set<Object> duplicateSet = new HashSet<>();
    private boolean traversalFunction = false;

    public DedupStep(final Traversal traversal) {
        super(traversal);
        DedupStep.generatePredicate(this);
    }

    public boolean hasUniqueFunction() {
        return null != this.uniqueFunction;
    }

    @Override
    public void addFunction(final Function<S, Object> function) {
        this.uniqueFunction = (this.traversalFunction = function instanceof TraversalObjectLambda) ?
                new TraversalLambda(((TraversalObjectLambda<S, Object>) function).getTraversal()) :
                function;
        DedupStep.generatePredicate(this);
    }

    @Override
    public List<Function<S, Object>> getFunctions() {
        return null == this.uniqueFunction ? Collections.emptyList() : Collections.singletonList(this.uniqueFunction);
    }

    @Override
    public Reducer<Set<Object>, S> getReducer() {
        return new Reducer<>(HashSet::new, (set, start) -> {
            set.add(null == this.uniqueFunction ? start : this.uniqueFunction.apply(start));
            return set;
        }, true);
    }

    @Override
    public DedupStep<S> clone() throws CloneNotSupportedException {
        final DedupStep<S> clone = (DedupStep<S>) super.clone();
        clone.duplicateSet = new HashSet<>();
        clone.uniqueFunction = CloneableLambda.cloneOrReturn(this.uniqueFunction);
        DedupStep.generatePredicate(clone);
        return clone;
    }

    @Override
    public void reset() {
        super.reset();
        this.duplicateSet.clear();
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.uniqueFunction);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
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
                return dedupStep.duplicateSet.add(dedupStep.uniqueFunction.apply(dedupStep.traversalFunction ? (S) traverser : traverser.get()));
            });
        }
    }
}
