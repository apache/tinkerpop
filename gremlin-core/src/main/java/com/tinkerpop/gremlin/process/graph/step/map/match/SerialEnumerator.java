package com.tinkerpop.gremlin.process.graph.step.map.match;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * An enumerator which consumes values from an iterator and maps each value to a secondary enumerator (for example, a join)
 * Enumerated indices cover all solutions in the secondary enumerators,
 * in ascending order according to the value iterator and the enumerators' own indices.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SerialEnumerator<T> implements Enumerator<T> {
    private final String name;
    private final Set<String> variables;
    private final Iterator<T> iterator;
    private final Function<T, Enumerator<T>> constructor;
    private final List<Enumerator<T>> memory = new ArrayList<>();
    private final List<T> values = new ArrayList<>();
    private int completedEnumsSize = 0; // TODO: this only assigned, not accessed until the efficient implementation of size() is restored

    public SerialEnumerator(final String name,
                            final Iterator<T> iterator,
                            final Function<T, Enumerator<T>> constructor) {
        this.name = name;
        this.iterator = iterator;
        this.constructor = constructor;
        this.variables = new HashSet<>();
        this.variables.add(name);
    }

    public Set<String> getVariables() {
        return variables;
    }

    @Override
    public int size() {
        // TODO: restore the more efficient implementation of size() while taking into account that
        // traversal iterators such as DefaultTraversal may return hasNext=true after first returning hasNext=false
        /*
        int size = completedEnumsSize;
        if (!sideEffects.isEmpty()) {
            size += sideEffects.get(sideEffects.size() - 1).size();
        }
        return size;
        */

        //*
        int size = 0;
        for (Enumerator<T> e : memory) size += e.size();
        return size;
        //*/
    }

    @Override
    public boolean isComplete() {
        return !iterator.hasNext() && (memory.isEmpty() || memory.get(memory.size() - 1).isComplete());
    }

    // note: *not* intended for random access; use binary search if this is ever needed
    @Override
    public boolean visitSolution(final int i,
                                 final BiConsumer<String, T> visitor) {
        int totalSize = 0;
        int index = 0;
        while (true) {
            if (index < memory.size()) {
                Enumerator<T> e = memory.get(index);

                if ((!e.isComplete() || e.isComplete() && i < totalSize + e.size()) && e.visitSolution(i - totalSize, visitor)) {
                    // additionally, bind the value stored in this enumerator
                    MatchStep.visit(name, values.get(index), visitor);

                    return true;
                }

                totalSize += e.size();
                index++;
            } else {
                if (!iterator.hasNext()) {
                    return false;
                }

                if (!memory.isEmpty()) {
                    int lastSize = memory.get(index - 1).size();

                    // first remove the head enumeration if it exists and is empty
                    // only the head will ever be empty, avoiding wasted space
                    if (0 == lastSize) {
                        index--;
                        memory.remove(index);
                        values.remove(index);
                    } else {
                        completedEnumsSize += lastSize;
                    }
                }

                T value = iterator.next();
                values.add(value);
                Enumerator<T> e = constructor.apply(value);
                memory.add(memory.size(), e);
            }
        }
    }
}
