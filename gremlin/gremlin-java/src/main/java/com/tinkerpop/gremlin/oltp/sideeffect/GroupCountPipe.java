package com.tinkerpop.gremlin.oltp.sideeffect;

import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.oltp.map.IdentityPipe;
import com.tinkerpop.gremlin.util.MapHelper;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroupCountPipe<S> extends IdentityPipe<S> {

    final Map<Object, Long> map;

    public GroupCountPipe(final Pipeline pipeline, final Map<Object, Long> map) {
        super(pipeline);
        this.map = map;
    }

    protected Holder<S> processNextStart() {
        final Holder<S> s = this.starts.next();
        MapHelper.incr(this.map, s.get(), 1l);
        return s;
    }
}
