package com.tinkerpop.gremlin.structure.server;

import com.tinkerpop.gremlin.structure.Element;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface ElementRange<U, E extends Element> {
    public Class<E> getElementType();

    public boolean contains(final U item);

    public int getPriority();
}
