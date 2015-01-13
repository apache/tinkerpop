package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class UnfoldStep<S, E> extends FlatMapStep<S, E> {

    public UnfoldStep(final Traversal traversal) {
        super(traversal);
        this.setFunction(traverser -> {
            final S s = traverser.get();
            if (s instanceof Iterator)
                return (Iterator) s;
            else if (s instanceof Iterable)
                return ((Iterable) s).iterator();
            else if (s instanceof Map)
                return ((Map) s).entrySet().iterator();
            else
                return IteratorUtils.of((E) s);
        });
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }
}
