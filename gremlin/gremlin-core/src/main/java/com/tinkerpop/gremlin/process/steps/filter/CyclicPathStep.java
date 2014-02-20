package com.tinkerpop.gremlin.process.steps.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.steps.PathConsumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CyclicPathStep<S> extends FilterStep<S> implements PathConsumer {

    public CyclicPathStep(final Traversal traversal) {
        super(traversal, holder -> !holder.getPath().isSimple());
    }
}
