package com.tinkerpop.gremlin.process.traverser;


import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.TraversalSideEffects;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.traverser.util.AbstractPathTraverser;
import com.tinkerpop.gremlin.process.util.SparsePath;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Map;
import java.util.Optional;
import java.util.WeakHashMap;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class B_O_PA_S_SE_SL_Traverser<T> extends AbstractPathTraverser<T> {

    protected B_O_PA_S_SE_SL_Traverser() {
    }

    public B_O_PA_S_SE_SL_Traverser(final T t, final Step<T, ?> step) {
        super(t, step);
        final Optional<String> stepLabel = step.getLabel();
        this.path = stepLabel.isPresent() ?
                getOrCreateFromCache(this.sideEffects).extend(t, stepLabel.get()) :
                getOrCreateFromCache(this.sideEffects).extend(t);
    }

    @Override
    public int hashCode() {
        return this.t.hashCode() + this.future.hashCode() + this.loops;
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof B_O_PA_S_SE_SL_Traverser
                && ((B_O_PA_S_SE_SL_Traverser) object).get().equals(this.t)
                && ((B_O_PA_S_SE_SL_Traverser) object).getFutureId().equals(this.getFutureId())
                && ((B_O_PA_S_SE_SL_Traverser) object).loops() == this.loops()
                && (null == this.sack);
    }

    @Override
    public Traverser.Admin<T> attach(final Vertex vertex) {
        super.attach(vertex);
        final Path newSparsePath = getOrCreateFromCache(this.sideEffects);
        this.path.forEach((object, labels) -> newSparsePath.extend(object, labels.toArray(new String[labels.size()])));
        this.path = newSparsePath;
        return this;
    }

    //////////////////////

    private static final Map<TraversalSideEffects, SparsePath> PATH_CACHE = new WeakHashMap<>();

    private static SparsePath getOrCreateFromCache(final TraversalSideEffects sideEffects) {
        SparsePath path = PATH_CACHE.get(sideEffects);
        if (null == path) {
            path = SparsePath.make();
            PATH_CACHE.put(sideEffects, path);
        }
        return path;
    }

}
