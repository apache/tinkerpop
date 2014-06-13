package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IdentityStep<S> extends FilterStep<S> implements Reversible {

    public IdentityStep(final Traversal traversal) {
        super(traversal);
        this.setPredicate(traverser -> true);
    }
}
