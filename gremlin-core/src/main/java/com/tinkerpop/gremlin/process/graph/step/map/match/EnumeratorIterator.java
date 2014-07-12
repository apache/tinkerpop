package com.tinkerpop.gremlin.process.graph.step.map.match;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.BiPredicate;

/**
* @author Joshua Shinavier (http://fortytwo.net)
*/
public class EnumeratorIterator<T> implements Iterator<Map<String, T>> {
    private final Enumerator<T> enumerator;
    private int index = 0;
    private Map<String, T> reuseMe = new HashMap<>();

    private BiPredicate<String, T> setCur = (s, t) -> {
        reuseMe.put(s, t);
        return true;
    };

    public EnumeratorIterator(Enumerator<T> enumerator) {
        this.enumerator = enumerator;
    }

    public boolean hasNext() {
        return index < enumerator.size() || !enumerator.isComplete();
    }

    public Map<String, T> next() {
        reuseMe.clear();
        if (!enumerator.visitSolution(index, setCur)) {
            throw new NoSuchElementException();
        }
        index++;
        return reuseMe;
    }
}
