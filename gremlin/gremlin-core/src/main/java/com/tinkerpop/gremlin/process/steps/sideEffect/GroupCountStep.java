package com.tinkerpop.gremlin.process.steps.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.steps.map.MapStep;
import com.tinkerpop.gremlin.process.steps.util.FunctionRing;
import com.tinkerpop.gremlin.process.steps.util.MapHelper;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroupCountStep<S> extends MapStep<S, S> {

    public final Map<Object, Long> map;
    public FunctionRing<S, ?> functionRing;

    public GroupCountStep(final Traversal traversal, final String variable, final Function<S, ?>... preGroupFunctions) {
        super(traversal);
        this.functionRing = new FunctionRing<>(preGroupFunctions);
        this.map = this.traversal.memory().getOrCreate(variable, HashMap<Object, Long>::new);
        this.setFunction(holder -> {
            final S s = holder.get();
            MapHelper.incr(this.map, this.functionRing.next().apply(s), 1l);
            return s;
        });
    }
}
