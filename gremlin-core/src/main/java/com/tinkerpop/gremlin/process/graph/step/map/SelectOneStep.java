package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.util.function.SFunction;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SelectOneStep<E> extends MapStep<Object, E> {

    public final String as;
    public final SFunction stepFunction;

    public SelectOneStep(final Traversal traversal, final String as, SFunction stepFunction) {
        super(traversal);
        this.as = as;
        this.stepFunction = stepFunction;
        this.setFunction(traverser -> {
            final Path path = traverser.hasPath() ? traverser.getPath() : null;
            final Object start = traverser.get();
            if (null != this.stepFunction) {
                if (null != path && path.hasAs(as))
                    return (E) this.stepFunction.apply(path.get(as));
                if (start instanceof Map && ((Map) start).containsKey(as))
                    return (E) this.stepFunction.apply(((Map) start).get(as));
            } else {
                if (null != path && path.hasAs(as))
                    return path.get(as);
                if (start instanceof Map && ((Map) start).containsKey(as))
                    return (E) ((Map) start).get(as);
            }
            return (E) NO_OBJECT;
        });
    }

    public SelectOneStep(final Traversal traversal, final String as) {
        this(traversal, as, null);
    }
}
