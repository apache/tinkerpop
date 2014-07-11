package com.tinkerpop.gremlin.process.graph.step.map.match;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiPredicate;

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

    public boolean isComplete() {
        return !iterator.hasNext();
    }

    public boolean visitSolution(int i, BiPredicate<String, T> visitor) {
        T value;

        if (i < size()) {
            value = memory.get(i);
        } else do {
            if (isComplete()) {
                return false;
            }

            value = iterator.next();
            memory.add(value);
        } while (i >= size());

        System.out.println(name + " visiting " + i + ": " + value);
        MatchStepNew.visit(name, value, visitor);

        return true;
    }
}
