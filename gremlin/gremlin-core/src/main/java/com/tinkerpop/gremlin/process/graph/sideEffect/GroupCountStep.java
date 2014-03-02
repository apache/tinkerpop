package com.tinkerpop.gremlin.process.graph.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.map.MapStep;
import com.tinkerpop.gremlin.process.util.FunctionRing;
import com.tinkerpop.gremlin.process.util.MapHelper;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroupCountStep<S> extends MapStep<S, S> implements SideEffectCapable {

    public final Map<Object, Long> groupCountMap;
    public FunctionRing<S, ?> functionRing;

    public GroupCountStep(final Traversal traversal, final Map<Object, Long> groupCountMap, final Function<S, ?>... preGroupFunctions) {
        super(traversal);
        this.functionRing = new FunctionRing<>(preGroupFunctions);
        this.groupCountMap = groupCountMap;
        this.traversal.memory().set(CAP_VARIABLE, this.groupCountMap);
        this.setFunction(holder -> {
            final S s = holder.get();
            MapHelper.incr(this.groupCountMap, this.functionRing.next().apply(s), 1l);
            return s;
        });
    }

    public GroupCountStep(final Traversal traversal, final String variable, final Function<S, ?>... preGroupFunctions) {
        this(traversal, traversal.memory().getOrCreate(variable, HashMap<Object, Long>::new), preGroupFunctions);
    }
}
