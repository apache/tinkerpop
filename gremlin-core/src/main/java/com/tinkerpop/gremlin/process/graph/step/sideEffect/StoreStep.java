package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Bulkable;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class StoreStep<S> extends FilterStep<S> implements Reversible, Bulkable {


    public StoreStep(final Traversal traversal) {
        super(traversal);
    }

}
