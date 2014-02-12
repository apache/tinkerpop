package com.tinkerpop.gremlin.process.oltp.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.GremlinHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BackStep<S, E> extends MapStep<S, E> {

    public String as;

    public BackStep(final Traversal traversal, final String as) {
        super(traversal);
        this.as = as;
        this.setFunction(holder -> holder.getPath().get(this.as));
    }

    public String toString() {
        return GremlinHelper.makePipeString(this, this.as);
    }
}
