package com.tinkerpop.gremlin.process.graph.step.map.match;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Function;

/**
* @author Joshua Shinavier (http://fortytwo.net)
*/
public class SerialEnumerator<T> implements Enumerator<T> {
    private final String name;
    private final Iterator<T> iterator;
    private final Function<T, Enumerator<T>> constructor;
    private final List<Enumerator<T>> memory = new ArrayList<>();
    private final List<T> values = new ArrayList<>();

    SerialEnumerator(final String name,
                     final Iterator<T> iterator,
                     final Function<T, Enumerator<T>> constructor) {
        this.name = name;
        this.iterator = iterator;
        this.constructor = constructor;
    }

    public int size() {
        // TODO: replace with an incremental size when done debugging (i.e. when size is under the control of this enumerator)
        int size = 0;
        for (Enumerator<T> e : memory) size += e.size();
        return size;
    }

    public boolean isComplete() {
        return !iterator.hasNext() && (memory.isEmpty() || memory.get(memory.size() - 1).isComplete());
    }

    public boolean visitSolution(final int i,
                                 final BiPredicate<String, T> visitor) {

        // TODO: temporary; replace with binary search for efficient random access
        int totalSize = 0;
        int index = 0;
        while (true) {
            if (index < memory.size()) {
                Enumerator<T> e = memory.get(index);

                if ((!e.isComplete() || e.isComplete() && i < totalSize + e.size()) && e.visitSolution(i - totalSize, visitor)) {
                    MatchStepNew.visit(name, values.get(index), visitor);

                    return true;
                }

                totalSize += e.size();
                index++;
            } else {
                if (!iterator.hasNext()) {
                    return false;
                }

                // first remove the head enumeration if it exists and is empty
                // only the head will ever be empty, avoiding wasted space
                if (!memory.isEmpty() && 0 == memory.get(index - 1).size()) {
                    index--;
                    memory.remove(index);
                    values.remove(index);
                }

                T value = iterator.next();
                values.add(value);
                Enumerator<T> e = constructor.apply(value);
                memory.add(memory.size(), e);
            }
        }
    }
}
