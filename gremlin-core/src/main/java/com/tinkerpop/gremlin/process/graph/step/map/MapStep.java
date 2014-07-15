package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.PathTraverser;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.util.function.SFunction;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MapStep<S, E> extends AbstractStep<S, E> {

    public SFunction<Traverser<S>, E> function;

    public MapStep(final Traversal traversal) {
        super(traversal);
    }

    protected Traverser<E> processNextStart() {
        while (true) {
            final Traverser<S> traverser = this.starts.next();
            final E newObject = this.function.apply(traverser);
            if (NO_OBJECT != newObject) {
                final S oldObject = traverser.get();
                if (oldObject.getClass().equals(newObject.getClass()) && oldObject.equals(newObject)) {
                    // no path extension (i.e. a filter, identity, side-effect)
                    if (traverser instanceof PathTraverser && TraversalHelper.isLabeled(this))
                        traverser.getPath().renameLastStep(this.getAs());
                    return (Traverser<E>) traverser;
                } else
                    return traverser.makeChild(this.getAs(), newObject);
            }
        }
    }

    public void setFunction(final SFunction<Traverser<S>, E> function) {
        this.function = function;
    }
}

