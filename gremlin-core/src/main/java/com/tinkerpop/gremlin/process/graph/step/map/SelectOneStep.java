package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.FunctionRing;
import com.tinkerpop.gremlin.util.function.SFunction;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SelectOneStep<E> extends MapStep<Object, E> {

    public final FunctionRing functionRing;
    public final String as;

    public SelectOneStep(final Traversal traversal, final String as, SFunction... stepFunctions) {
        super(traversal);
        this.functionRing = new FunctionRing(stepFunctions);
        this.as = as;
        this.setFunction(traverser -> {
            final Path path = traverser.hasPath() ? traverser.getPath() : null;
            final Object start = traverser.get();
            if (this.functionRing.hasFunctions()) {
                if (null != path && path.hasAs(as))
                    return (E) this.functionRing.next().apply(path.get(as));
                if (start instanceof Map && ((Map) start).containsKey(as))
                    return (E) this.functionRing.next().apply(((Map) start).get(as));
            } else {
                if (null != path && path.hasAs(as))
                    return path.get(as);
                if (start instanceof Map && ((Map) start).containsKey(as))
                    return (E) ((Map) start).get(as);
            }
            this.functionRing.reset();
            return (E) NO_OBJECT;
        });
    }
}
