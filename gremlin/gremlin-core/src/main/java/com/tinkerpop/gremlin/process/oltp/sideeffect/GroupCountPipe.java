package com.tinkerpop.gremlin.process.oltp.sideeffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.oltp.map.MapPipe;
import com.tinkerpop.gremlin.process.oltp.util.FunctionRing;
import com.tinkerpop.gremlin.process.oltp.util.MapHelper;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroupCountPipe<S> extends MapPipe<S, S> {

    public final Map<Object, Long> map;
    public FunctionRing<S, ?> functionRing;

    public GroupCountPipe(final Traversal pipeline, final String variable, final Function<S, ?>... preGroupFunctions) {
        super(pipeline);
        this.functionRing = new FunctionRing<>(preGroupFunctions);
        this.map = this.pipeline.memory().getOrCreate(variable, HashMap<Object, Long>::new);
        this.setFunction(holder -> {
            final S s = holder.get();
            MapHelper.incr(this.map, this.functionRing.next().apply(s), 1l);
            return s;
        });
    }
}
