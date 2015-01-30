package com.tinkerpop.gremlin.process.graph.traversal.step.branch;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.traversal.__;
import com.tinkerpop.gremlin.process.graph.marker.TraversalOptionHolder;
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Collections;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class UnionStep<S, E> extends BranchStep<S, E, TraversalOptionHolder.Pick> {

    public UnionStep(final Traversal traversal, final Traversal<?, E>... unionTraversals) {
        super(traversal);
        this.setBranchTraversal(__.<S, Pick>map(t -> Pick.any).asAdmin());
        for (final Traversal<?, E> union : unionTraversals) {
            this.addOption(Pick.any, (Traversal<S, E>) union);
        }
    }

    @Override
    public void addOption(final Pick pickToken, final Traversal<S, E> traversalOption) {
        if (Pick.any != pickToken)
            throw new IllegalArgumentException("Union step only supports the any token: " + pickToken);
        super.addOption(pickToken, traversalOption);
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.traversalOptions.getOrDefault(Pick.any, Collections.emptyList()));
    }
}
