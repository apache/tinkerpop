package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traversal;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalRing<S, E> implements Cloneable {

    private Traversal<S, E>[] traversals;
    private int currentTraversal = -1;

    public TraversalRing(final Traversal<S, E>... traversals) {
        this.traversals = traversals;
    }

    public Traversal<S, E> next() {
        this.currentTraversal = (this.currentTraversal + 1) % this.traversals.length;
        return this.traversals[this.currentTraversal];
    }

    public void reset() {
        this.currentTraversal = -1;
    }

    public int size() {
        return this.traversals.length;
    }

    public List<Traversal<S, E>> getTraversals() {
        return Arrays.asList(this.traversals);
    }

    public void forEach(final Consumer<Traversal<S, E>> consumer) {
        for (int i = 0; i < this.traversals.length; i++) {
            consumer.accept(this.traversals[i]);
        }
    }

    @Override
    public TraversalRing<S, E> clone() throws CloneNotSupportedException {
        final Traversal<S, E>[] clonedRing = new Traversal[this.traversals.length];
        for (int i = 0; i < this.traversals.length; i++) {
            clonedRing[i] = this.traversals[i].clone();
        }
        return new TraversalRing(clonedRing);
    }

}
