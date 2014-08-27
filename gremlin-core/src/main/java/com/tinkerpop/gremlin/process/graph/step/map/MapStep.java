package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.util.function.SBiFunction;
import com.tinkerpop.gremlin.util.function.SFunction;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MapStep<S, E> extends AbstractStep<S, E> {

    public SFunction<Traverser<S>, E> function = null;
    public SBiFunction<Traverser<S>, Traversal.SideEffects, E> biFunction = null;

    public MapStep(final Traversal traversal) {
        super(traversal);
    }

    protected Traverser<E> processNextStart() {
        while (true) {
            final Traverser<S> traverser = this.starts.next();
            final E end = (null == this.biFunction) ?
                    this.function.apply(traverser) :
                    this.biFunction.apply(traverser, this.traversal.sideEffects());
            if (NO_OBJECT != end) return traverser.makeChild(this.getLabel(), end);
        }
    }

    public void setFunction(final SFunction<Traverser<S>, E> function) {
        this.function = function;
    }

    public void setBiFunction(final SBiFunction<Traverser<S>, Traversal.SideEffects, E> biFunction) {
        this.biFunction = biFunction;
    }
}

