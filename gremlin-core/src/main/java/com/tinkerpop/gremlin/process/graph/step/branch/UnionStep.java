package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.TraversalOptionHolder;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Collections;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class UnionStep<S, E> extends BranchStep<S, E, TraversalOptionHolder.Pick> {

    public UnionStep(final Traversal traversal, final Traversal<S, E>... unionTraversals) {
        super(traversal);
        this.setFunction(traverser -> Pick.any);
        for (final Traversal<S, E> union : unionTraversals) {
            super.addOption(Pick.any, union);
        }
    }

    @Override
    public void addOption(final Pick pickToken, final Traversal<S, E> traversalOption) {
        if (Pick.any != pickToken)
            throw new IllegalArgumentException("Union step only supports the 'any' pick token: " + pickToken);
        super.addOption(pickToken, traversalOption);
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.traversalOptions.getOrDefault(Pick.any, Collections.emptyList()));
    }

    /*@Override
    protected Iterator<Traverser<E>> standardAlgorithm() {
        while (true) {
            for (final Traversal<S, E> union : this.unionTraversals) {
                if (union.hasNext()) return union.asAdmin().getEndStep();
            }
            final Traverser.Admin<S> start = this.starts.next();
            this.unionTraversals.forEach(union -> union.asAdmin().addStart(start.split()));
        }
    }

    @Override
    protected Iterator<Traverser<E>> computerAlgorithm() {
        final List<Traverser<E>> ends = new ArrayList<>();
        while (ends.isEmpty()) {
            final Traverser.Admin<S> start = this.starts.next();
            for (final Traversal<S, E> union : this.unionTraversals) {
                final Traverser.Admin<S> unionSplit = start.split();
                unionSplit.setStepId(union.asAdmin().getStartStep().getId());
                ends.add((Traverser) unionSplit);
            }
        }
        return ends.iterator();
    }*/
}
