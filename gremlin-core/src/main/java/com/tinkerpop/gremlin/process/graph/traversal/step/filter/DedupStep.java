package com.tinkerpop.gremlin.process.graph.traversal.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Reducing;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.traversal.TraversalParent;
import com.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import com.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class DedupStep<S> extends FilterStep<S> implements Reversible, Reducing<Set<Object>, S>, TraversalParent {

    private Traversal.Admin<S, Object> dedupTraversal = new IdentityTraversal<>();
    private Set<Object> duplicateSet = new HashSet<>();

    public DedupStep(final Traversal traversal) {
        super(traversal);
        DedupStep.generatePredicate(this);
    }


    @Override
    public List<Traversal<S, Object>> getLocalChildren() {
        return Collections.singletonList(this.dedupTraversal);
    }

    @Override
    public void addLocalChild(final Traversal.Admin dedupTraversal) {
        this.dedupTraversal = this.integrateChild(dedupTraversal, TYPICAL_LOCAL_OPERATIONS);
    }

    @Override
    public Reducer<Set<Object>, S> getReducer() {
        return new Reducer<>(HashSet::new, (set, start) -> {
            set.add(TraversalUtil.apply(start, this.dedupTraversal));
            return set;
        }, true);
    }

    @Override
    public DedupStep<S> clone() throws CloneNotSupportedException {
        final DedupStep<S> clone = (DedupStep<S>) super.clone();
        clone.duplicateSet = new HashSet<>();
        clone.dedupTraversal = clone.integrateChild(this.dedupTraversal.clone(), TYPICAL_LOCAL_OPERATIONS);
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
        return TraversalHelper.makeStepString(this, this.dedupTraversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.SIDE_EFFECTS);
    }

    /////////////////////////

    private static final <S> void generatePredicate(final DedupStep<S> dedupStep) {
        dedupStep.setPredicate(traverser -> {
            traverser.asAdmin().setBulk(1);
            return dedupStep.duplicateSet.add(TraversalUtil.apply(traverser.asAdmin(), dedupStep.dedupTraversal));
        });
    }
}
