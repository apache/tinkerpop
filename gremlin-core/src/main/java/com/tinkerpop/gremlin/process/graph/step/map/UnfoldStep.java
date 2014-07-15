package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.SingleIterator;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class UnfoldStep<S, E> extends FlatMapStep<S, E> {

    public UnfoldStep(final Traversal traversal) {
        super(traversal);
        this.setFunction(traverser -> {
            final S s = traverser.get();
            if (s instanceof Iterator)
                return (Iterator) s;
            else if (s instanceof Iterable)
                return ((Iterable) s).iterator();
            else
                return new SingleIterator(s);
        });
    }
}
