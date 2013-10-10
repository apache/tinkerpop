package com.tinkerpop.gremlin.pipes;

import com.tinkerpop.gremlin.pipes.util.Holder;

import java.io.Serializable;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Pipe<S, E> extends Iterator<Holder<E>>, Serializable {

    public static final Object NO_OBJECT = new Object();
    public static final String NONE = "none";

    public void addStarts(Iterator<Holder<S>> iterator);

    public <P extends Pipeline> P getPipeline();

    public String getName();

    public void setName(String name);

    // TODO: public void reset();
}
