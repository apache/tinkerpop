package com.tinkerpop.gremlin.process.steps.filter;


import com.tinkerpop.gremlin.process.Traversal;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SimplePathStep<S> extends FilterStep<S> {

    public SimplePathStep(final Traversal traversal) {
        super(traversal, holder -> holder.getPath().isSimple());
    }
}
