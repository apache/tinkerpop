package com.tinkerpop.gremlin.server.util.ser;

import com.tinkerpop.gremlin.server.Context;
import com.tinkerpop.gremlin.server.ResultCode;
import com.tinkerpop.gremlin.server.ResultSerializer;

/**
 * Serialize results via toString.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ToStringResultSerializer implements ResultSerializer {

    private static final String TEXT_RESPONSE_FORMAT_WITH_RESULT = "%s>>%s";
    private static final String TEXT_RESPONSE_FORMAT_WITH_NULL = "%s>>null";

    @Override
    public String serialize(final Object o, final ResultCode code, final Context context) {
        return o == null ?
                String.format(TEXT_RESPONSE_FORMAT_WITH_NULL, context.getRequestMessage().requestId) :
                String.format(TEXT_RESPONSE_FORMAT_WITH_RESULT, context.getRequestMessage().requestId, o.toString());
    }

    @Override
    public String[] mimeTypesSupported() {
        return new String[] {"text/plain"};
    }
}


