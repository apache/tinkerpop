package com.tinkerpop.gremlin.process.graph.map;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.PathHolder;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.util.function.SFunction;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MapStep<S, E> extends AbstractStep<S, E> {

    public SFunction<Holder<S>, E> function;

    public MapStep(final Traversal traversal) {
        super(traversal);
    }

    public MapStep(final Traversal traversal, final SFunction<Holder<S>, E> function) {
        super(traversal);
        this.function = function;
    }

    protected Holder<E> processNextStart() {
        while (true) {
            final Holder<S> holder = this.starts.next();
            final E temp = this.function.apply(holder);
            if (NO_OBJECT != temp)
                if (holder.get().equals(temp)) {// no path extension (i.e. a filter, identity, side-effect)
                    if (holder instanceof PathHolder && TraversalHelper.isLabeled(this)) {
                        final Holder<E> sibling = (Holder<E>) holder.makeSibling();
                        sibling.getPath().renameLastStep(this.getAs());
                        return sibling;
                    } else
                        return (Holder<E>) holder.makeSibling();
                } else
                    return holder.makeChild(this.getAs(), temp);
        }
    }

    public void setFunction(final SFunction<Holder<S>, E> function) {
        this.function = function;
    }
}

