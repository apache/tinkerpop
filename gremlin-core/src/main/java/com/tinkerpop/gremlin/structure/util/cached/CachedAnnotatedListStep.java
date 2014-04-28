package com.tinkerpop.gremlin.structure.util.cached;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.map.FlatMapStep;
import com.tinkerpop.gremlin.structure.AnnotatedList;
import com.tinkerpop.gremlin.structure.AnnotatedValue;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class CachedAnnotatedListStep<V> extends FlatMapStep<AnnotatedList<V>, AnnotatedValue<V>> {

    public CachedAnnotatedListStep(final Traversal traversal) {
        super(traversal);
        this.setFunction(holder -> {
            final CachedAnnotatedList list = (CachedAnnotatedList) holder.get();
            return (Iterator) new ArrayList<AnnotatedValue>(list.annotatedValues).iterator();
        });
    }
}
