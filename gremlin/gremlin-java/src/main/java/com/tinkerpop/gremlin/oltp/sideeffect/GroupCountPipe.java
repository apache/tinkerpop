package com.tinkerpop.gremlin.oltp.sideeffect;

import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.oltp.map.MapPipe;
import com.tinkerpop.gremlin.util.FunctionRing;
import com.tinkerpop.gremlin.util.GremlinHelper;
import com.tinkerpop.gremlin.util.MapHelper;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroupCountPipe<S> extends MapPipe<S, S> {

    public final Map<Object, Long> map;
    public FunctionRing<S, ?> functionRing;

    public GroupCountPipe(final Pipeline pipeline, final String variable, final Function<S, ?>... preGroupFunctions) {
        super(pipeline);
        this.functionRing = new FunctionRing<>(preGroupFunctions);
        this.map = GremlinHelper.getOrCreate(this.pipeline, variable, HashMap<Object, Long>::new);
        this.setFunction(holder -> {
            final S s = holder.get();
            MapHelper.incr(this.map, this.functionRing.next().apply(s), 1l);
            return s;
        });
    }
}
