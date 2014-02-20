package com.tinkerpop.tinkergraph.process.steps.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.steps.map.FlatMapStep;
import com.tinkerpop.gremlin.structure.AnnotatedList;
import com.tinkerpop.gremlin.structure.AnnotatedValue;
import com.tinkerpop.tinkergraph.TinkerAnnotatedList;
import com.tinkerpop.tinkergraph.TinkerHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerAnnotatedListStep<V> extends FlatMapStep<AnnotatedList<V>, AnnotatedValue<V>> {

    public TinkerAnnotatedListStep(final Traversal traversal) {
        super(traversal);
        this.setFunction(holder -> TinkerHelper.getAnnotatedValues((TinkerAnnotatedList) holder.get()));
    }
}
