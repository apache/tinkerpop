package com.tinkerpop.gremlin.process.graph.step.map.match;

import java.util.Iterator;
import java.util.function.BiConsumer;

/**
 * An enumerator of at most one element
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SimpleEnumerator<T> implements Enumerator<T> {

    private final String name;
    private Iterator<T> iterator;
    private T element;

    public SimpleEnumerator(final String name,
                            final Iterator<T> iterator) {
        this.name = name;
        this.iterator = iterator;
    }

    @Override
    public int size() {
        return null == element ? 0 : 1;
    }

    @Override
    public boolean visitSolution(int index, BiConsumer<String, T> visitor) {
        if (0 != index) {
            return false;
        }

        if (null != iterator) {
            if (iterator.hasNext()) {
                element = iterator.next();
            }
            iterator = null;
        }

        if (null != element) {
            MatchStep.visit(name, element, visitor);
            return true;
        }

        return false;
    }
}
