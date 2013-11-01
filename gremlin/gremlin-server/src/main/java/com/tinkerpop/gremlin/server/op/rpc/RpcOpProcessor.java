package com.tinkerpop.gremlin.server.op.rpc;

import com.tinkerpop.gremlin.server.Context;
import com.tinkerpop.gremlin.server.OpProcessor;
import com.tinkerpop.gremlin.server.RequestMessage;

import java.util.function.Consumer;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class RpcOpProcessor implements OpProcessor {
    private static final String OP_PROCESSOR_NAME = "rpc";

    @Override
    public String getName() {
        return OP_PROCESSOR_NAME;
    }

    @Override
    public Consumer<Context> select(RequestMessage message) {
        return c -> {};
    }
}
