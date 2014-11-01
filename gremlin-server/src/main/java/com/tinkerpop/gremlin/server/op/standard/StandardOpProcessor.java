package com.tinkerpop.gremlin.server.op.standard;

import com.tinkerpop.gremlin.server.Context;
import com.tinkerpop.gremlin.server.OpProcessor;
import com.tinkerpop.gremlin.server.op.AbstractEvalOpProcessor;
import com.tinkerpop.gremlin.util.function.ThrowingConsumer;

/**
 * Simple {@link OpProcessor} implementation that handles {@code ScriptEngine} script evaluation outside the context
 * of a session.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class StandardOpProcessor extends AbstractEvalOpProcessor {
    public static final String OP_PROCESSOR_NAME = "";

    @Override
    public String getName() {
        return OP_PROCESSOR_NAME;
    }

    @Override
    public ThrowingConsumer<Context> getEvalOp() {
        return StandardOps::evalOp;
    }
}
