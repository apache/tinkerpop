package com.tinkerpop.gremlin.process.computer;

/**
 * A MessageCombiner allows two messages in route to the same vertex to be aggregated into a single message.
 * Message combining can reduce the number of messages sent between vertices and thus, reduce network traffic.
 * Not all messages can be combined and thus, this is an optional feature of a {@link VertexProgram}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface MessageCombiner<M> {

    /**
     * Combine two messages and return a message containing the combination.
     * In many instances, it is possible to simply merge the data in the second message into the first message.
     * Such an optimization can limit the amount of object creation.
     *
     * @param messageA the first message
     * @param messageB the second message
     * @return the combination of the two messages
     */
    public M combine(final M messageA, final M messageB);
}
