package com.tinkerpop.gremlin.server.handler;

import com.tinkerpop.gremlin.driver.MessageSerializer;
import io.netty.util.AttributeKey;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class StateKey {
    public static final AttributeKey<MessageSerializer> SERIALIZER = AttributeKey.valueOf("serializer");
    public static final AttributeKey<Boolean> USE_BINARY = AttributeKey.valueOf("useBinary");
}
