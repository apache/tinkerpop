package com.tinkerpop.gremlin.pipes.map;

import com.tinkerpop.gremlin.MapPipe;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.pipes.util.GremlinHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BackPipe<S> extends MapPipe<S, Object> {

    public final String as;

    public BackPipe(final Pipeline pipeline, final String as) {
        super(pipeline, o -> o.getPath().get(as));
        this.as = as;
    }

    public String toString() {
        return GremlinHelper.makePipeString(this, this.as);
    }
}
