package com.tinkerpop.gremlin.process.computer;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface MessageCombiner<M> {

    public Iterator<M> combine(final M previousMessage, final M newMessage);
}
