package com.tinkerpop.gremlin.server.util.ser;

import com.tinkerpop.gremlin.server.Context;
import com.tinkerpop.gremlin.server.RequestMessage;
import com.tinkerpop.gremlin.server.ResultCode;
import com.tinkerpop.gremlin.server.MessageSerializer;

import java.util.Optional;

/**
 * Serialize results via {@link Object#toString}.  It is important to note that this serializer does not support
 * {@link RequestMessage} deserialization which means that requests cannot be submitted with this type.  This
 * {@link MessageSerializer} can only format results to this format.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ToStringMessageSerializer implements MessageSerializer {

    private static final String TEXT_RESPONSE_FORMAT_WITH_RESULT = "%s>>%s";
    private static final String TEXT_RESPONSE_FORMAT_WITH_NULL = "%s>>null";

    @Override
    public String serialize(final Object o) {
        return o.toString();
    }

    @Override
    public String serializeResult(final Object o, final ResultCode code, final Context context) {
        return o == null ?
                String.format(TEXT_RESPONSE_FORMAT_WITH_NULL, context.getRequestMessage().requestId) :
                String.format(TEXT_RESPONSE_FORMAT_WITH_RESULT, context.getRequestMessage().requestId, o.toString());
    }

    @Override
    public Optional<RequestMessage> deserializeRequest(String msg) {
        throw new UnsupportedOperationException(String.format("The %s does not support the %s format.",
                ToStringMessageSerializer.class.getName(), RequestMessage.class.getName()));
    }

    @Override
    public String[] mimeTypesSupported() {
        return new String[] {"text/plain"};
    }
}


