package com.tinkerpop.gremlin.process.traversal.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraversalRing<A, B> {

    private final IdentityTraversal<A, B> identityTraversal = new IdentityTraversal<>();

    private List<Traversal.Admin<A, B>> traversals = new ArrayList<>();
    private int currentTraversal = -1;

    public TraversalRing(final Traversal.Admin<A,B>... traversals) {
        this.traversals = new ArrayList<>(Arrays.asList(traversals));
    }

    public Traversal.Admin<A, B> next() {
        if (this.traversals.size() == 0) {
            return this.identityTraversal;
        } else {
            this.currentTraversal = (this.currentTraversal + 1) % this.traversals.size();
            return this.traversals.get(this.currentTraversal);
        }
    }

    public boolean isEmpty() {
        return this.traversals.isEmpty();
    }

    public void reset() {
        this.currentTraversal = -1;
    }

    public int size() {
        return this.traversals.size();
    }

    public void addTraversal(final Traversal.Admin<A, B> traversal) {
        this.traversals.add(traversal);
    }

    public List<Traversal.Admin<A, B>> getTraversals() {
        return Collections.unmodifiableList(this.traversals);
    }

    @Override
    public String toString() {
        return this.traversals.toString();
    }
}
