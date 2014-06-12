package com.tinkerpop.gremlin.process.computer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface MessageCombiner<M> {

    public M combine(final M previousMessage, final M newMessage);
}
