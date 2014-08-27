package com.tinkerpop.gremlin.process.graph.step.map.match;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;

/**
* @author Joshua Shinavier (http://fortytwo.net)
*/
public class IteratorEnumerator<T> implements Enumerator<T> {
    private final String name;
    private final Iterator<T> iterator;
    private final List<T> memory = new ArrayList<>();

    public IteratorEnumerator(final String name,
                              final Iterator<T> iterator) {
        this.name = name;
        this.iterator = iterator;
    }

    public int size() {
        return memory.size();
    }

    // Returns true as soon as iterator.hasNext() returns false.
    // Note that DefaultTraversal has the behavior of returning true after it has first returned false,
    // but that we reset traversals between IteratorEnumerator lifetimes.
    public boolean isComplete() {
        return !iterator.hasNext();
    }

    public boolean visitSolution(final int index,
                                 final BiConsumer<String, T> visitor) {
        T value;

        if (index < memory.size()) {
            value = memory.get(index);
        } else do {
            if (!iterator.hasNext()) {
                return false;
            }

            value = iterator.next();
            memory.add(value);
        } while (index >= memory.size());

        MatchStep.visit(name, value, visitor);

        return true;
    }
}
