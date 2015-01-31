package com.tinkerpop.gremlin.server.handler;

import com.tinkerpop.gremlin.driver.MessageSerializer;
import com.tinkerpop.gremlin.server.op.session.Session;
import io.netty.util.AttributeKey;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class StateKey {
    public static final AttributeKey<MessageSerializer> SERIALIZER = AttributeKey.valueOf("serializer");
    public static final AttributeKey<Boolean> USE_BINARY = AttributeKey.valueOf("useBinary");
    public static final AttributeKey<Session> SESSION = AttributeKey.valueOf("session");
}
