package com.tinkerpop.gremlin.olap;

import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.PathHolder;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GremlinMessage implements Serializable {

    protected Holder holder;

    protected GremlinMessage(final Holder holder) {
        this.holder = holder;
        this.holder.deflate();
        if (this.holder instanceof PathHolder)
            this.holder.getPath().deflate();
    }

    public static <T extends GremlinMessage> T of(final Holder holder) {
        if (holder instanceof PathHolder)
            return (T) GremlinPathMessage.of(holder);
        else
            return (T) GremlinCounterMessage.of(holder);
    }
}