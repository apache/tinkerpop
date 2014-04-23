package com.tinkerpop.gremlin.driver.ser;

import com.tinkerpop.gremlin.driver.MessageSerializer;
import com.tinkerpop.gremlin.driver.message.RequestMessage;
import com.tinkerpop.gremlin.driver.message.ResponseMessage;

import java.util.Optional;

/**
 * An extension to the MessageSerializer interface that allows a format to be compatible with text-based
 * websocket messages.  This interface is for internal purposes only.  Implementers who have custom serialization
 * needs should NOT implement this interface as it will not be used.  Gremlin Server only supports plain JSON
 * for text-based requests.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface MessageTextSerializer extends MessageSerializer {
    public String serializeResponseAsString(final ResponseMessage responseMessage);
    public String serializeRequestAsString(final RequestMessage requestMessage);
    public Optional<RequestMessage> deserializeRequest(final String msg);
    public Optional<ResponseMessage> deserializeResponse(final String msg);
}
