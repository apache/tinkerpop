package com.tinkerpop.gremlin.oltp.sideeffect;

import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.oltp.AbstractPipe;
import com.tinkerpop.gremlin.util.GremlinHelper;
import com.tinkerpop.gremlin.util.MapHelper;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroupCountPipe<S> extends AbstractPipe<S, S> {

    public final Map<Object, Long> map;
    public final Function<S, ?>[] preGroupFunctions;
    public int currentFunction = 0;

    public GroupCountPipe(final Pipeline pipeline, final String variable, final Function<S, ?>... preGroupFunctions) {
        super(pipeline);
        this.preGroupFunctions = preGroupFunctions;
        this.map = GremlinHelper.getOrCreate(this.pipeline, variable, HashMap<Object, Long>::new);
    }

    protected Holder<S> processNextStart() {
        final Holder<S> s = this.starts.next();
        if (this.preGroupFunctions.length == 0)
            MapHelper.incr(this.map, s.get(), 1l);
        else {
            MapHelper.incr(this.map, this.preGroupFunctions[this.currentFunction].apply(s.get()), 1l);
            this.currentFunction = (this.currentFunction + 1) % this.preGroupFunctions.length;
        }
        return s;
    }
}
