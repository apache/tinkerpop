package com.tinkerpop.gremlin.process.steps.util;

import com.tinkerpop.gremlin.process.Traversal;

import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalRing<S, E> {

    public Traversal<S, E>[] traversals;
    private int currentPipeline = -1;

    public TraversalRing(final Traversal<S, E>... traversals) {
        this.traversals = traversals;
    }

    public Traversal<S, E> next() {
        this.currentPipeline = (this.currentPipeline + 1) % this.traversals.length;
        return this.traversals[this.currentPipeline];
    }

    public void reset() {
        this.currentPipeline = -1;
    }

    public int size() {
        return this.traversals.length;
    }

    public Stream<Traversal<S, E>> stream() {
        return Stream.of(this.traversals);
    }

    public void forEach(final Consumer<Traversal<S, E>> consumer) {
        for (int i = 0; i < this.traversals.length; i++) {
            consumer.accept(this.traversals[i]);
        }
    }

}
