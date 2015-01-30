package com.tinkerpop.gremlin.process.graph.traversal.step.branch;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.TraversalOptionParent;
import com.tinkerpop.gremlin.process.traversal.lambda.MapTraversal;
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Collections;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class UnionStep<S, E> extends BranchStep<S, E, TraversalOptionParent.Pick> {

    public UnionStep(final Traversal traversal, final Traversal.Admin<?, E>... unionTraversals) {
        super(traversal);
        this.setBranchTraversal(new MapTraversal<>(s -> Pick.any));
        for (final Traversal.Admin<?, E> union : unionTraversals) {
            this.addGlobalChildOption(Pick.any, (Traversal.Admin) union);
        }
    }

    @Override
    public void addGlobalChildOption(final Pick pickToken, final Traversal.Admin<S, E> traversalOption) {
        if (Pick.any != pickToken)
            throw new IllegalArgumentException("Union step only supports the any token: " + pickToken);
        super.addGlobalChildOption(pickToken, traversalOption);
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.traversalOptions.getOrDefault(Pick.any, Collections.emptyList()));
    }
}
