package com.tinkerpop.gremlin.process.graph.step.map.match;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

/**
* @author Joshua Shinavier (http://fortytwo.net)
*/
public class IteratorEnumerator<T> implements Enumerator<T> {
    private final String name;
    private final Iterator<T> iterator;
    private final List<T> memory = new ArrayList<>();
    private final Set<String> variables;

    public IteratorEnumerator(final String name,
                              final Iterator<T> iterator) {
        this.name = name;
        this.iterator = iterator;
        this.variables = new HashSet<>();
        this.variables.add(name);
    }

    public Set<String> getVariables() {
        return variables;
    }

    public int size() {
        return memory.size();
    }

    // Returns true as soon as iterator.hasNext() returns false.
    // Note that DefaultTraversal has the behavior of returning true after it has first returned false,
    // but that we exhaust traversals between IteratorEnumerator lifetimes.
    public boolean isComplete() {
        return !iterator.hasNext();
    }

    public boolean visitSolution(int index, BiConsumer<String, T> visitor) {
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

        MatchStepNew.visit(name, value, visitor);

        return true;
    }
}
