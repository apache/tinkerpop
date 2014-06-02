package com.tinkerpop.gremlin.process.graph.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.PathConsumer;
import com.tinkerpop.gremlin.process.util.Reversible;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CyclicPathStep<S> extends FilterStep<S> implements PathConsumer, Reversible {

    public CyclicPathStep(final Traversal traversal) {
        super(traversal);
        this.setPredicate(traverser -> !traverser.getPath().isSimple());
    }
}
