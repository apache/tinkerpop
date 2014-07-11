package com.tinkerpop.gremlin.process.graph.step.map.match;

import java.util.function.BiPredicate;

/**
* @author Joshua Shinavier (http://fortytwo.net)
*/
public interface Enumerator<T> {
    int size();

    boolean isComplete();

    boolean visitSolution(int i, BiPredicate<String, T> visitor);
}
