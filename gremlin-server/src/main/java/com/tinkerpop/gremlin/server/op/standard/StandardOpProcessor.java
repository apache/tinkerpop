package com.tinkerpop.gremlin.server.op.standard;

import com.tinkerpop.gremlin.driver.Tokens;
import com.tinkerpop.gremlin.driver.message.RequestMessage;
import com.tinkerpop.gremlin.driver.message.ResponseMessage;
import com.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.server.Context;
import com.tinkerpop.gremlin.server.Graphs;
import com.tinkerpop.gremlin.server.OpProcessor;
import com.tinkerpop.gremlin.server.op.OpProcessorException;
import com.tinkerpop.gremlin.util.function.ThrowingConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Simple {@link OpProcessor} implementation that handles {@code ScriptEngine} script evaluation outside the context
 * of a session.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class StandardOpProcessor implements OpProcessor {
    private static final Logger logger = LoggerFactory.getLogger(StandardOpProcessor.class);
    public static final String OP_PROCESSOR_NAME = "";

    /**
     * This may or may not be the full set of invalid binding keys.  It is dependent on the static imports made to
     * Gremlin Server.  This should get rid of the worst offenders though and provide a good message back to the
     * calling client.
     */
    private static final List<String> invalidBindingsKeys = Arrays.asList(
            T.id.getAccessor(), T.key.getAccessor(),
            T.label.getAccessor(), T.value.getAccessor());
    private static final String invalidBindingKeysJoined = String.join(",", invalidBindingsKeys);

    @Override
    public String getName() {
        return OP_PROCESSOR_NAME;
    }

    @Override
    public ThrowingConsumer<Context> select(final Context ctx) throws OpProcessorException {
        final RequestMessage message = ctx.getRequestMessage();
        logger.debug("Selecting processor for RequestMessage {}", message);

        final ThrowingConsumer<Context> op;
        switch (message.getOp()) {
            case Tokens.OPS_EVAL:
                op = validateEvalMessage(message).orElse(StandardOps::evalOp);
                break;
            case Tokens.OPS_INVALID:
                final String msgInvalid = String.format("Message could not be parsed.  Check the format of the request. [%s]", message);
                throw new OpProcessorException(msgInvalid, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_MALFORMED_REQUEST).result(msgInvalid).create());
            default:
                final String msgDefault = String.format("Message with op code [%s] is not recognized.", message.getOp());
                throw new OpProcessorException(msgDefault, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_MALFORMED_REQUEST).result(msgDefault).create());
        }

        return op;
    }

    private static Optional<ThrowingConsumer<Context>> validateEvalMessage(final RequestMessage message) throws OpProcessorException {
        if (!message.optionalArgs(Tokens.ARGS_GREMLIN).isPresent()) {
            final String msg = String.format("A message with an [%s] op code requires a [%s] argument.", Tokens.OPS_EVAL, Tokens.ARGS_GREMLIN);
            throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).result(msg).create());
        }

        if (message.optionalArgs(Tokens.ARGS_BINDINGS).isPresent()) {
            final Map<String,Object> bindings = (Map<String, Object>) message.getArgs().get(Tokens.ARGS_BINDINGS);
            if (bindings.keySet().stream().anyMatch(invalidBindingsKeys::contains)) {
                final String msg = String.format("The [%s] message is using at least one of the invalid binding key of [%s]. It conflicts with standard static imports to Gremlin Server.", Tokens.OPS_EVAL, invalidBindingKeysJoined);
                throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).result(msg).create());
            }
        }

        return Optional.empty();
    }
}
