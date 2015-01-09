package com.tinkerpop.gremlin.driver.ser;

import com.tinkerpop.gremlin.driver.MessageSerializer;
import com.tinkerpop.gremlin.driver.message.RequestMessage;
import com.tinkerpop.gremlin.driver.message.ResponseMessage;

/**
 * An extension to the MessageSerializer interface that allows a format to be compatible with text-based
 * websocket messages.  This interface is for internal purposes only.  Implementers who have mapper serialization
 * needs should NOT implement this interface as it will not be used.  Gremlin Server only supports plain JSON
 * for text-based requests.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface MessageTextSerializer extends MessageSerializer {
    public String serializeResponseAsString(final ResponseMessage responseMessage) throws SerializationException;

    public String serializeRequestAsString(final RequestMessage requestMessage) throws SerializationException;

    public RequestMessage deserializeRequest(final String msg) throws SerializationException;

    public ResponseMessage deserializeResponse(final String msg) throws SerializationException;
}
