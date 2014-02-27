package com.tinkerpop.tinkergraph;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.map.StartStep;
import com.tinkerpop.gremlin.process.graph.DefaultGraphTraversal;
import com.tinkerpop.gremlin.structure.AnnotatedList;
import com.tinkerpop.gremlin.structure.AnnotatedValue;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.tinkergraph.process.graph.map.TinkerAnnotatedListStep;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerAnnotatedList<V> implements AnnotatedList<V>, Serializable {

    protected final List<AnnotatedValue<V>> annotatedValues = new ArrayList<>();

    public AnnotatedValue<V> addValue(final V value, final Object... annotationKeyValues) {
        final AnnotatedValue<V> annotatedValue = new TinkerAnnotatedValue<V>(value, annotationKeyValues) {
            public void remove() {
                annotatedValues.remove(this);
            }
        };
        this.annotatedValues.add(annotatedValue);
        return annotatedValue;
    }

    public GraphTraversal<AnnotatedList<V>, AnnotatedValue<V>> annotatedValues() {
        final GraphTraversal<AnnotatedList<V>, AnnotatedValue<V>> traversal = new DefaultGraphTraversal<>();
        traversal.addStep(new StartStep<AnnotatedList>(traversal, this));
        traversal.addStep(new TinkerAnnotatedListStep<V>(traversal));
        return traversal;
    }

    public String toString() {
        return StringFactory.annotatedListString(this);
    }
}

