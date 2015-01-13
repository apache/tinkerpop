package com.tinkerpop.gremlin.process.traverser;

import com.tinkerpop.gremlin.process.traverser.util.AbstractTraverser;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class O_Traverser<T> extends AbstractTraverser<T> {

    protected O_Traverser() {
    }

    public O_Traverser(final T t) {
        super(t);
    }

}
