package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.util.AbstractStep;

import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MapStep<S, E> extends AbstractStep<S, E> {

    private Function<Traverser<S>, E> function = null;

    public MapStep(final Traversal traversal) {
        super(traversal);
    }

    @Override
    protected Traverser<E> processNextStart() {
        while (true) {
            final Traverser.System<S> traverser = this.starts.next();
            final E end = this.function.apply(traverser);
            if (NO_OBJECT != end) return traverser.makeChild(this.getLabel(), end);
        }
    }

    public void setFunction(final Function<Traverser<S>, E> function) {
        this.function = function;
    }
}

