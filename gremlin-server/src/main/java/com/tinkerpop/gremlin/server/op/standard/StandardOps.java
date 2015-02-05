package com.tinkerpop.gremlin.server.op.standard;

import com.tinkerpop.gremlin.driver.Tokens;
import com.tinkerpop.gremlin.driver.message.RequestMessage;
import com.tinkerpop.gremlin.server.Context;
import com.tinkerpop.gremlin.server.op.AbstractEvalOpProcessor;
import com.tinkerpop.gremlin.server.op.OpProcessorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.SimpleBindings;
import java.util.Map;
import java.util.Optional;

/**
 * Operations to be used by the {@link StandardOpProcessor}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
final class StandardOps {
    private static final Logger logger = LoggerFactory.getLogger(StandardOps.class);

    public static void evalOp(final Context context) throws OpProcessorException {
        final RequestMessage msg = context.getRequestMessage();
        AbstractEvalOpProcessor.evalOp(context, context::getGremlinExecutor, () -> {
            final Bindings bindings = new SimpleBindings();
            Optional.ofNullable((Map<String, Object>) msg.getArgs().get(Tokens.ARGS_BINDINGS)).ifPresent(bindings::putAll);
            return bindings;
        });
    }
}
