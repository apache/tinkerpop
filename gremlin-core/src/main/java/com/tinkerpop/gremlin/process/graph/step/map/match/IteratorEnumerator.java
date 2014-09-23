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
    private Iterator<T> iterator;
    private final List<T> memory = new ArrayList<>();

    public IteratorEnumerator(final String name,
                              final Iterator<T> iterator) {
        this.name = name;
        this.iterator = iterator;
    }

    public int size() {
        return memory.size();
    }

    public boolean visitSolution(final int index,
                                 final BiConsumer<String, T> visitor) {
        T value;

        if (index < memory.size()) {
            value = memory.get(index);
        } else do {
            if (null == iterator) {
                return false;
            } else if (!iterator.hasNext()) {
                // free up memory as soon as possible
                iterator = null;
                return false;
            }

            value = iterator.next();
            memory.add(value);
        } while (index >= memory.size());

        MatchStep.visit(name, value, visitor);

        return true;
    }
}
