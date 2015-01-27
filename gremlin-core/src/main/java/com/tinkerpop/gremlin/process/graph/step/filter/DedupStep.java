package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.FunctionHolder;
import com.tinkerpop.gremlin.process.graph.marker.Reducing;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.SmartLambda;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class DedupStep<S> extends FilterStep<S> implements Reversible, Reducing<Set<Object>, S>, FunctionHolder<S, Object>, TraversalHolder {

    private static final Set<TraverserRequirement> REQUIREMENTS = new HashSet<>(Arrays.asList(
            TraverserRequirement.BULK,
            TraverserRequirement.OBJECT
    ));

    private SmartLambda<S, Object> smartLambda = null;
    private Set<Object> duplicateSet = new HashSet<>();

    public DedupStep(final Traversal traversal) {
        super(traversal);
        DedupStep.generatePredicate(this);
    }

    @Override
    public void addFunction(final Function<S, Object> function) {
        this.smartLambda = new SmartLambda<>(function);
        this.executeTraversalOperations(this.smartLambda.getTraversal(), TYPICAL_LOCAL_OPERATIONS);
        DedupStep.generatePredicate(this);
    }

    @Override
    public List<Function<S, Object>> getFunctions() {
        return null == this.smartLambda ? Collections.emptyList() : Collections.singletonList(this.smartLambda);
    }

    @Override
    public List<Traversal<S, Object>> getLocalTraversals() {
        return null == this.smartLambda ? Collections.emptyList() : this.smartLambda.getTraversalAsList();
    }

    @Override
    public Reducer<Set<Object>, S> getReducer() {
        return new Reducer<>(HashSet::new, (set, start) -> {
            set.add(null == this.smartLambda ? start : this.smartLambda.apply((S) start));
            return set;
        }, true);
    }

    @Override
    public DedupStep<S> clone() throws CloneNotSupportedException {
        final DedupStep<S> clone = (DedupStep<S>) super.clone();
        clone.duplicateSet = new HashSet<>();
        if (null != this.smartLambda) {
            clone.smartLambda = this.smartLambda.clone();
            clone.executeTraversalOperations(clone.smartLambda.getTraversal(), TYPICAL_LOCAL_OPERATIONS);
        }
        DedupStep.generatePredicate(clone);
        return clone;
    }

    @Override
    public void reset() {
        super.reset();
        if (null != this.smartLambda) this.smartLambda.reset();
        this.duplicateSet.clear();
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.smartLambda);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        if (null == this.smartLambda)
            return REQUIREMENTS;
        else {
            final Set<TraverserRequirement> requirements = TraversalHolder.super.getRequirements();
            requirements.addAll(REQUIREMENTS);
            return requirements;
        }
    }

    /////////////////////////

    private static final <S> void generatePredicate(final DedupStep<S> dedupStep) {
        if (null == dedupStep.smartLambda) {
            dedupStep.setPredicate(traverser -> {
                traverser.asAdmin().setBulk(1);
                return dedupStep.duplicateSet.add(traverser.get());
            });
        } else {
            dedupStep.setPredicate(traverser -> {
                traverser.asAdmin().setBulk(1);
                return dedupStep.duplicateSet.add(dedupStep.smartLambda.apply((S) traverser));
            });
        }
    }
}
