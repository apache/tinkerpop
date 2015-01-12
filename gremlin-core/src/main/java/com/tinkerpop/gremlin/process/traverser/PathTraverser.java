package com.tinkerpop.gremlin.process.traverser;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.util.ImmutablePath;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PathTraverser<T> extends AbstractTraverser<T> {

    protected PathTraverser() {
    }

    public PathTraverser(final T t, final Step<T, ?> step) {
        super(t, step);
        this.path = new ImmutablePath(step.getLabel(), t);
    }

    @Override
    public int hashCode() {
        return super.hashCode() + this.path.hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return (object instanceof PathTraverser)
                && ((PathTraverser) object).path().equals(this.path) // TODO: path equality
                && ((PathTraverser) object).get().equals(this.t)
                && ((PathTraverser) object).getFuture().equals(this.getFuture())
                && ((PathTraverser) object).loops() == this.loops()
                && (null == this.sack);
    }
}
