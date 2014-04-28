package com.tinkerpop.gremlin.structure.util.cached;

import com.tinkerpop.gremlin.process.graph.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.map.StartStep;
import com.tinkerpop.gremlin.structure.AnnotatedList;
import com.tinkerpop.gremlin.structure.AnnotatedValue;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.List;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class CachedAnnotatedList<V> implements AnnotatedList<V> {

    protected List<AnnotatedValue<V>> annotatedValues;

    public CachedAnnotatedList(final List<AnnotatedValue<V>> annotatedValues) {
        this.annotatedValues = annotatedValues;
    }

    public AnnotatedValue<V> addValue(final V value, final Object... annotationKeyValues) {
        throw new UnsupportedOperationException("Cached annotations are readonly: " + this.toString());
    }

    public GraphTraversal<AnnotatedList<V>, AnnotatedValue<V>> annotatedValues() {
        final GraphTraversal<AnnotatedList<V>, AnnotatedValue<V>> traversal = new DefaultGraphTraversal<>();
        traversal.addStep(new StartStep<AnnotatedList>(traversal, this));
        traversal.addStep(new CachedAnnotatedListStep<V>(traversal));
        return traversal;
    }

    public String toString() {
        return StringFactory.annotatedListString(this);
    }
}