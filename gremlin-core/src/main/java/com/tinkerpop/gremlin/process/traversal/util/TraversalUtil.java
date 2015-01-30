package com.tinkerpop.gremlin.process.traversal.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;

import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraversalUtil {

    private TraversalUtil() {
    }

    public static final <S, E> E apply(final Traverser.Admin<S> traverser, final Traversal.Admin<S, E> traversal) {
        final Traverser.Admin<S> split = traverser.split();
        split.setSideEffects(traversal.getSideEffects());
        split.setBulk(1l); // TODO: do we do this?
        traversal.reset();
        traversal.addStart(split);
        try {
            return traversal.next(); // map
        } catch (final NoSuchElementException e) {
            throw new IllegalArgumentException("The provided traverser does not map to a value: " + split + "->" + traversal);
        }
    }

    public static final <S, E> boolean test(final Traverser.Admin<S> traverser, final Traversal.Admin<S, E> traversal) {
        final Traverser.Admin<S> split = traverser.split();
        split.setSideEffects(traversal.getSideEffects());
        split.setBulk(1l); // TODO: do we do this?
        traversal.reset();
        traversal.addStart(split);
        return traversal.hasNext(); // filter
    }

    ///////

    public static final <S, E> E apply(final S start, final Traversal.Admin<S, E> traversal) {
        traversal.reset();
        traversal.addStart(traversal.getTraverserGenerator().generate(start, traversal.getStartStep(), 1l));
        try {
            return traversal.next(); // map
        } catch (final NoSuchElementException e) {
            throw new IllegalArgumentException("The provided start does not map to a value: " + start + "->" + traversal);
        }
    }

    public static final <S, E> boolean test(final S start, final Traversal.Admin<S, E> traversal) {
        traversal.reset();
        traversal.addStart(traversal.getTraverserGenerator().generate(start, traversal.getStartStep(), 1l));
        return traversal.hasNext(); // filter
    }

}
