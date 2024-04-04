/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.server.handler;

import com.codahale.metrics.Timer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.util.ReferenceCountUtil;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.groovy.jsr223.TimedInterruptTimeoutException;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngine;
import org.apache.tinkerpop.gremlin.jsr223.JavaTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Failure;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.util.BytecodeHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.ProcessingException;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.apache.tinkerpop.gremlin.server.util.TextPlainMessageSerializer;
import org.apache.tinkerpop.gremlin.server.util.TraverserIterator;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.TemporaryException;
import org.apache.tinkerpop.gremlin.util.ExceptionHelper;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.util.ser.MessageChunkSerializer;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;
import org.codehaus.groovy.control.MultipleCompilationErrorsException;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.codahale.metrics.MetricRegistry.name;
import static io.netty.handler.codec.http.HttpHeaderNames.TRANSFER_ENCODING;
import static io.netty.handler.codec.http.HttpHeaderValues.CHUNKED;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONTINUE;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.apache.tinkerpop.gremlin.server.handler.HttpGremlinEndpointHandler.RequestState.CHUNKING_NOT_SUPPORTED;
import static org.apache.tinkerpop.gremlin.server.handler.HttpGremlinEndpointHandler.RequestState.FINISHED;
import static org.apache.tinkerpop.gremlin.server.handler.HttpGremlinEndpointHandler.RequestState.FINISHING;
import static org.apache.tinkerpop.gremlin.server.handler.HttpGremlinEndpointHandler.RequestState.NOT_STARTED;
import static org.apache.tinkerpop.gremlin.server.handler.HttpGremlinEndpointHandler.RequestState.STREAMING;
import static org.apache.tinkerpop.gremlin.server.handler.HttpHandlerUtil.sendTrailingHeaders;
import static org.apache.tinkerpop.gremlin.server.handler.HttpHandlerUtil.writeError;

/**
 * Handler that processes HTTP requests to the HTTP Gremlin endpoint.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@ChannelHandler.Sharable
public class HttpGremlinEndpointHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(HttpGremlinEndpointHandler.class);
    private static final Logger auditLogger = LoggerFactory.getLogger(GremlinServer.AUDIT_LOGGER_NAME);

    private static final Timer evalOpTimer = MetricManager.INSTANCE.getTimer(name(GremlinServer.class, "op", "eval"));

    private static final Bindings EMPTY_BINDINGS = new SimpleBindings();

    protected static final Set<String> INVALID_BINDINGS_KEYS = new HashSet<>();

    static {
        INVALID_BINDINGS_KEYS.addAll(Arrays.asList(
                T.id.name(), T.key.name(),
                T.label.name(), T.value.name(),
                T.id.getAccessor(), T.key.getAccessor(),
                T.label.getAccessor(), T.value.getAccessor(),
                T.id.getAccessor().toUpperCase(), T.key.getAccessor().toUpperCase(),
                T.label.getAccessor().toUpperCase(), T.value.getAccessor().toUpperCase()));

        for (Column enumItem : Column.values()) {
            INVALID_BINDINGS_KEYS.add(enumItem.name());
        }

        for (Order enumItem : Order.values()) {
            INVALID_BINDINGS_KEYS.add(enumItem.name());
        }

        for (Operator enumItem : Operator.values()) {
            INVALID_BINDINGS_KEYS.add(enumItem.name());
        }

        for (Scope enumItem : Scope.values()) {
            INVALID_BINDINGS_KEYS.add(enumItem.name());
        }

        for (Pop enumItem : Pop.values()) {
            INVALID_BINDINGS_KEYS.add(enumItem.name());
        }
    }

    /**
     * Serializers for the response.
     */
    private final Map<String, MessageSerializer<?>> serializers;

    /**
     * Serializer for {@code text/plain} which is a serializer exclusive to HTTP.
     */
    private static final TextPlainMessageSerializer textPlainSerializer = new TextPlainMessageSerializer();

    private final GremlinExecutor gremlinExecutor;
    private final GraphManager graphManager;
    private final Settings settings;

    private static final Pattern pattern = Pattern.compile("(.*);q=(.*)");

    public HttpGremlinEndpointHandler(final Map<String, MessageSerializer<?>> serializers,
                                      final GremlinExecutor gremlinExecutor,
                                      final GraphManager graphManager,
                                      final Settings settings) {
        this.serializers = serializers;
        this.gremlinExecutor = gremlinExecutor;
        this.graphManager = graphManager;
        this.settings = settings;
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
        if (msg instanceof FullHttpRequest) {
            final FullHttpRequest req = (FullHttpRequest) msg;
            final boolean keepAlive = HttpUtil.isKeepAlive(req);

            if (HttpUtil.is100ContinueExpected(req)) {
                ctx.write(new DefaultFullHttpResponse(HTTP_1_1, CONTINUE));
            }

            if (req.method() != POST) {
                HttpHandlerUtil.sendError(ctx, METHOD_NOT_ALLOWED, METHOD_NOT_ALLOWED.toString(), keepAlive);
                ReferenceCountUtil.release(msg);
                return;
            }

            final RequestMessage requestMessage;
            try {
                requestMessage = HttpHandlerUtil.getRequestMessageFromHttpRequest(req, serializers);
            } catch (IllegalArgumentException | SerializationException | NullPointerException ex) {
                HttpHandlerUtil.sendError(ctx, BAD_REQUEST, ex.getMessage(), keepAlive);
                ReferenceCountUtil.release(msg);
                return;
            }

            final UUID requestId = requestMessage.getRequestId();
            final String acceptMime = Optional.ofNullable(req.headers().get(HttpHeaderNames.ACCEPT)).orElse("application/json");
            final Pair<String, MessageSerializer<?>> serializer = chooseSerializer(acceptMime);
            if (null == serializer) {
                HttpHandlerUtil.sendError(ctx, BAD_REQUEST, requestId, String.format("no serializer for requested Accept header: %s", acceptMime),
                        keepAlive);
                ReferenceCountUtil.release(msg);
                return;
            }

            final String origin = req.headers().get(HttpHeaderNames.ORIGIN);

            // not using the req anywhere below here - assume it is safe to release at this point.
            ReferenceCountUtil.release(msg);

            final RequestState requestState = serializer.getValue1() instanceof MessageChunkSerializer
                    ? NOT_STARTED
                    : CHUNKING_NOT_SUPPORTED;

            final Context requestCtx = new Context(requestMessage, ctx, settings, graphManager, gremlinExecutor,
                    gremlinExecutor.getScheduledExecutorService(), requestState);

            final Timer.Context timerContext = evalOpTimer.time();
            // timeout override - handle both deprecated and newly named configuration. earlier logic should prevent
            // both configurations from being submitted at the same time
            final Map<String, Object> args = requestMessage.getArgs();
            final long seto = args.containsKey(Tokens.ARGS_EVAL_TIMEOUT) ?
                    ((Number) args.get(Tokens.ARGS_EVAL_TIMEOUT)).longValue() : requestCtx.getSettings().getEvaluationTimeout();

            final FutureTask<Void> evalFuture = new FutureTask<>(() -> {
                requestCtx.setStartedResponse();

                try {
                    logger.debug("Processing request containing script [{}] and bindings of [{}] on {}",
                            requestMessage.getArgOrDefault(Tokens.ARGS_GREMLIN, ""),
                            requestMessage.getArgOrDefault(Tokens.ARGS_BINDINGS, Collections.emptyMap()),
                            Thread.currentThread().getName());
                    if (settings.enableAuditLog) {
                        AuthenticatedUser user = ctx.channel().attr(StateKey.AUTHENTICATED_USER).get();
                        if (null == user) {    // This is expected when using the AllowAllAuthenticator
                            user = AuthenticatedUser.ANONYMOUS_USER;
                        }
                        String address = ctx.channel().remoteAddress().toString();
                        if (address.startsWith("/") && address.length() > 1) address = address.substring(1);
                        auditLogger.info("User {} with address {} requested: {}", user.getName(), address,
                                requestMessage.getArgOrDefault(Tokens.ARGS_GREMLIN, ""));
                    }

                    // Send back the 200 OK response header here since the response is always chunk transfer encoded. Any
                    // failures that follow this will show up in the response body instead.
                    final HttpResponse responseHeader = new DefaultHttpResponse(HTTP_1_1, OK);
                    responseHeader.headers().set(TRANSFER_ENCODING, CHUNKED);
                    responseHeader.headers().set(HttpHeaderNames.CONTENT_TYPE, serializer.getValue0());
                    HttpUtil.setKeepAlive(responseHeader, keepAlive);
                    if (origin != null) {
                        responseHeader.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, origin);
                    }
                    ctx.writeAndFlush(responseHeader);

                    try {
                        switch (requestMessage.getOp()) {
                            case "":
                            case Tokens.OPS_EVAL:
                                iterateScriptEvalResult(requestCtx, serializer.getValue1(), requestMessage);
                                break;
                            case Tokens.OPS_BYTECODE:
                                iterateTraversal(requestCtx, serializer.getValue1(), translateBytecodeToTraversal(requestCtx));
                                break;
                            case Tokens.OPS_INVALID:
                                final String msgInvalid =
                                        String.format("Message could not be parsed. Check the format of the request. [%s]", requestMessage);
                                throw new ProcessingException(msgInvalid,
                                        ResponseMessage.build(requestMessage)
                                                .code(ResponseStatusCode.REQUEST_ERROR_MALFORMED_REQUEST)
                                                .statusMessage(msgInvalid)
                                                .create());
                            default:
                                final String msgDefault =
                                        String.format("Message with op code [%s] is not recognized.", requestMessage.getOp());
                                throw new ProcessingException(msgDefault,
                                        ResponseMessage.build(requestMessage)
                                                .code(ResponseStatusCode.REQUEST_ERROR_MALFORMED_REQUEST)
                                                .statusMessage(msgDefault)
                                                .create());
                        }
                    } catch (ProcessingException ope) {
                        logger.warn(ope.getMessage(), ope);
                        writeError(requestCtx, ope.getResponseMessage(), serializer.getValue1());
                    }
                } catch (Exception ex) {
                    // send the error response here and don't rely on exception caught because it might not have the
                    // context on whether to close the connection or not, based on keepalive.
                    final Throwable t = ExceptionHelper.getRootCause(ex);
                    if (t instanceof TooLongFrameException) {
                        writeError(requestCtx,
                                ResponseMessage.build(requestId)
                                        .code(ResponseStatusCode.SERVER_ERROR)
                                        .statusMessage(t.getMessage() + " - increase the maxContentLength")
                                        .create(),
                                serializer.getValue1());
                    } else {
                        writeError(requestCtx,
                                ResponseMessage.build(requestId)
                                        .code(ResponseStatusCode.SERVER_ERROR)
                                        .statusMessage((t != null) ? t.getMessage() : ex.getMessage())
                                        .create(),
                                serializer.getValue1());
                    }
                } finally {
                    timerContext.stop();

                    // There is a race condition that this query may have finished before the timeoutFuture was created,
                    // though this is very unlikely. This is handled in the settor, if this has already been grabbed.
                    // If we passed this point and the setter hasn't been called, it will cancel the timeoutFuture inside
                    // the setter to compensate.
                    final ScheduledFuture<?> timeoutFuture = requestCtx.getTimeoutExecutor();
                    if (null != timeoutFuture)
                        timeoutFuture.cancel(true);
                }

                return null;
            });

            try {
                final Future<?> executionFuture = requestCtx.getGremlinExecutor().getExecutorService().submit(evalFuture);
                if (seto > 0) {
                    // Schedule a timeout in the thread pool for future execution
                    requestCtx.setTimeoutExecutor(requestCtx.getScheduledExecutorService().schedule(() -> {
                        executionFuture.cancel(true);
                        if (!requestCtx.getStartedResponse()) {
                            final String errorMessage = String.format("A timeout occurred during traversal evaluation of [%s] - consider increasing the limit given to evaluationTimeout", requestMessage);
                            writeError(requestCtx,
                                    ResponseMessage.build(requestMessage)
                                            .code(ResponseStatusCode.SERVER_ERROR_TIMEOUT)
                                            .statusMessage(errorMessage)
                                            .create(),
                                    serializer.getValue1());
                        }
                    }, seto, TimeUnit.MILLISECONDS));
                }
            } catch (RejectedExecutionException ree) {
                writeError(requestCtx,
                        ResponseMessage.build(requestMessage).code(ResponseStatusCode.TOO_MANY_REQUESTS).statusMessage("Rate limiting").create(),
                        serializer.getValue1());
            }
        }
    }

    private void iterateScriptEvalResult(final Context context, MessageSerializer<?> serializer, final RequestMessage message)
            throws ProcessingException, InterruptedException, ScriptException {
        if (!message.optionalArgs(Tokens.ARGS_GREMLIN).isPresent()) {
            final String msg = String.format("A message with an [%s] op code requires a [%s] argument.", Tokens.OPS_EVAL, Tokens.ARGS_GREMLIN);
            throw new ProcessingException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
        }

        if (message.optionalArgs(Tokens.ARGS_BINDINGS).isPresent()) {
            final Map bindings = (Map) message.getArgs().get(Tokens.ARGS_BINDINGS);
            if (IteratorUtils.anyMatch(bindings.keySet().iterator(), k -> null == k || !(k instanceof String))) {
                final String msg = String.format("The [%s] message is using one or more invalid binding keys - they must be of type String and cannot be null", Tokens.OPS_EVAL);
                throw new ProcessingException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
            }

            final Set<String> badBindings = IteratorUtils.set(IteratorUtils.<String>filter(bindings.keySet().iterator(), INVALID_BINDINGS_KEYS::contains));
            if (!badBindings.isEmpty()) {
                final String msg = String.format("The [%s] message supplies one or more invalid parameters key of [%s] - these are reserved names.", Tokens.OPS_EVAL, badBindings);
                throw new ProcessingException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
            }

            // ignore control bindings that get passed in with the "#jsr223" prefix - those aren't used in compilation
            if (IteratorUtils.count(IteratorUtils.filter(bindings.keySet().iterator(), k -> !k.toString().startsWith("#jsr223"))) > settings.maxParameters) {
                final String msg = String.format("The [%s] message contains %s bindings which is more than is allowed by the server %s configuration",
                        Tokens.OPS_EVAL, bindings.size(), settings.maxParameters);
                throw new ProcessingException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
            }
        }

        final Map<String, Object> args = message.getArgs();
        final String language = args.containsKey(Tokens.ARGS_LANGUAGE) ? (String) args.get(Tokens.ARGS_LANGUAGE) : "gremlin-groovy";
        final GremlinScriptEngine scriptEngine = gremlinExecutor.getScriptEngineManager().getEngineByName(language);

        final Bindings bindings = mergeBindingsFromRequest(context, graphManager.getAsBindings());
        final Object result = scriptEngine.eval((String) message.getArg(Tokens.ARGS_GREMLIN), bindings);

        handleIterator(context, IteratorUtils.asIterator(result), serializer);
    }

    private static Traversal.Admin<?,?> translateBytecodeToTraversal(Context ctx) throws ProcessingException {
        final RequestMessage requestMsg = ctx.getRequestMessage();

        if (!requestMsg.optionalArgs(Tokens.ARGS_GREMLIN).isPresent()) {
            final String msg = String.format("A message with [%s] op code requires a [%s] argument.", Tokens.OPS_BYTECODE, Tokens.ARGS_GREMLIN);
            throw new ProcessingException(msg, ResponseMessage.build(requestMsg).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
        }

        if (!(requestMsg.optionalArgs(Tokens.ARGS_GREMLIN).get() instanceof Bytecode)) {
            final String msg = String.format("A message with [%s] op code requires a [%s] argument that is of type %s.",
                    Tokens.OPS_BYTECODE, Tokens.ARGS_GREMLIN, Bytecode.class.getSimpleName());
            throw new ProcessingException(msg, ResponseMessage.build(requestMsg).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
        }

        final Optional<Map<String, String>> aliases = requestMsg.optionalArgs(Tokens.ARGS_ALIASES);
        if (!aliases.isPresent()) {
            final String msg = String.format("A message with [%s] op code requires a [%s] argument.", Tokens.OPS_BYTECODE, Tokens.ARGS_ALIASES);
            throw new ProcessingException(msg, ResponseMessage.build(requestMsg).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
        }

        if (aliases.get().size() != 1 || !aliases.get().containsKey(Tokens.VAL_TRAVERSAL_SOURCE_ALIAS)) {
            final String msg = String.format("A message with [%s] op code requires the [%s] argument to be a Map containing one alias assignment named '%s'.",
                    Tokens.OPS_BYTECODE, Tokens.ARGS_ALIASES, Tokens.VAL_TRAVERSAL_SOURCE_ALIAS);
            throw new ProcessingException(msg, ResponseMessage.build(requestMsg).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
        }

        final String traversalSourceBindingForAlias = aliases.get().values().iterator().next();
        if (!ctx.getGraphManager().getTraversalSourceNames().contains(traversalSourceBindingForAlias)) {
            final String msg = String.format("The traversal source [%s] for alias [%s] is not configured on the server.", traversalSourceBindingForAlias, Tokens.VAL_TRAVERSAL_SOURCE_ALIAS);
            throw new ProcessingException(msg, ResponseMessage.build(requestMsg).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
        }

        final String traversalSourceName = aliases.get().entrySet().iterator().next().getValue();
        final TraversalSource g = ctx.getGraphManager().getTraversalSource(traversalSourceName);
        final Bytecode bytecode = (Bytecode) requestMsg.getArgs().get(Tokens.ARGS_GREMLIN); // type checked at start of method.
        try {
            final Optional<String> lambdaLanguage = BytecodeHelper.getLambdaLanguage(bytecode);
            if (!lambdaLanguage.isPresent())
                return JavaTranslator.of(g).translate(bytecode);
            else
                return ctx.getGremlinExecutor().eval(bytecode, EMPTY_BINDINGS, lambdaLanguage.get(), traversalSourceName);
        } catch (ScriptException ex) {
            logger.error("Traversal contains a lambda that cannot be compiled", ex);
            throw new ProcessingException("Traversal contains a lambda that cannot be compiled",
                    ResponseMessage.build(requestMsg).code(ResponseStatusCode.SERVER_ERROR_EVALUATION)
                            .statusMessage(ex.getMessage())
                            .statusAttributeException(ex).create());
        } catch (Exception ex) {
            logger.error("Could not deserialize the Traversal instance", ex);
            throw new ProcessingException("Could not deserialize the Traversal instance",
                    ResponseMessage.build(requestMsg).code(ResponseStatusCode.SERVER_ERROR_SERIALIZATION)
                            .statusMessage(ex.getMessage())
                            .statusAttributeException(ex).create());
        }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
        logger.error("Error processing HTTP Request", cause);

        if (ctx.channel().isActive()) {
            HttpHandlerUtil.sendError(ctx, INTERNAL_SERVER_ERROR, cause.getMessage(), false);
        }
    }

    private Bindings mergeBindingsFromRequest(final Context ctx, final Bindings bindings) throws ProcessingException {
        // alias any global bindings to a different variable.
        final RequestMessage msg = ctx.getRequestMessage();
        if (msg.getArgs().containsKey(Tokens.ARGS_ALIASES)) {
            final Map<String, String> aliases = (Map<String, String>) msg.getArgs().get(Tokens.ARGS_ALIASES);
            for (Map.Entry<String, String> aliasKv : aliases.entrySet()) {
                boolean found = false;

                // first check if the alias refers to a Graph instance
                final Graph graph = ctx.getGraphManager().getGraph(aliasKv.getValue());
                if (null != graph) {
                    bindings.put(aliasKv.getKey(), graph);
                    found = true;
                }

                // if the alias wasn't found as a Graph then perhaps it is a TraversalSource - it needs to be
                // something
                if (!found) {
                    final TraversalSource ts = ctx.getGraphManager().getTraversalSource(aliasKv.getValue());
                    if (null != ts) {
                        bindings.put(aliasKv.getKey(), ts);
                        found = true;
                    }
                }

                // this validation is important to calls to GraphManager.commit() and rollback() as they both
                // expect that the aliases supplied are valid
                if (!found) {
                    final String error = String.format("Could not alias [%s] to [%s] as [%s] not in the Graph or TraversalSource global bindings",
                            aliasKv.getKey(), aliasKv.getValue(), aliasKv.getValue());
                    throw new ProcessingException(error, ResponseMessage.build(msg)
                            .code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(error).create());
                }
            }
        }

        // add any bindings to override any other supplied
        Optional.ofNullable((Map<String, Object>) msg.getArgs().get(Tokens.ARGS_BINDINGS)).ifPresent(bindings::putAll);
        return bindings;
    }

    private Pair<String, MessageSerializer<?>> chooseSerializer(final String mimeType) {
        final List<Pair<String, Double>> ordered = Stream.of(mimeType.split(",")).map(mediaType -> {
            // parse out each mediaType with its params - keeping it simple and just looking for "quality".  if
            // that value isn't there, default it to 1.0.  not really validating here so users better get their
            // accept headers straight
            final Matcher matcher = pattern.matcher(mediaType);
            return (matcher.matches()) ? Pair.with(matcher.group(1), Double.parseDouble(matcher.group(2))) : Pair.with(mediaType, 1.0);
        }).sorted((o1, o2) -> o2.getValue0().compareTo(o1.getValue0())).collect(Collectors.toList());

        for (Pair<String, Double> p : ordered) {
            // this isn't perfect as it doesn't really account for wildcards.  that level of complexity doesn't seem
            // super useful for gremlin server really.
            final String accept = p.getValue0().equals("*/*") ? "application/json" : p.getValue0();
            if (serializers.containsKey(accept))
                return Pair.with(accept, serializers.get(accept));
            else if (accept.equals("text/plain")) {
                return Pair.with(accept, textPlainSerializer);
            }
        }

        return null;
    }

    private void iterateTraversal(final Context context, MessageSerializer<?> serializer, Traversal.Admin<?, ?> traversal) {
        final RequestMessage msg = context.getRequestMessage();
        logger.debug("Traversal request {} for in thread {}", msg.getRequestId(), Thread.currentThread().getName());

        try {
            try {
                // compile the traversal - without it getEndStep() has nothing in it
                traversal.applyStrategies();
                handleIterator(context, new TraverserIterator(traversal), serializer);
            } catch (Exception ex) {
                Throwable t = ex;
                if (ex instanceof UndeclaredThrowableException)
                    t = t.getCause();

                // if any exception in the chain is TemporaryException or Failure then we should respond with the
                // right error code so that the client knows to retry
                final Optional<Throwable> possibleSpecialException = determineIfSpecialException(ex);
                if (possibleSpecialException.isPresent()) {
                    final Throwable special = possibleSpecialException.get();
                    final ResponseMessage.Builder specialResponseMsg = ResponseMessage.build(msg).
                            statusMessage(special.getMessage()).
                            statusAttributeException(special);
                    if (special instanceof TemporaryException) {
                        specialResponseMsg.code(ResponseStatusCode.SERVER_ERROR_TEMPORARY);
                    } else if (special instanceof Failure) {
                        final Failure failure = (Failure) special;
                        specialResponseMsg.code(ResponseStatusCode.SERVER_ERROR_FAIL_STEP).
                                statusAttribute(Tokens.STATUS_ATTRIBUTE_FAIL_STEP_MESSAGE, failure.format());
                    }
                    writeError(context, specialResponseMsg.create(), serializer);
                } else if (t instanceof InterruptedException || t instanceof TraversalInterruptedException) {
                    final String errorMessage = String.format("A timeout occurred during traversal evaluation of [%s] - consider increasing the limit given to evaluationTimeout", msg);
                    logger.warn(errorMessage);
                    writeError(context,
                            ResponseMessage.build(msg)
                                    .code(ResponseStatusCode.SERVER_ERROR_TIMEOUT)
                                    .statusMessage(errorMessage)
                                    .statusAttributeException(ex)
                                    .create(),
                            serializer);
                } else if (t instanceof TimedInterruptTimeoutException) {
                    // occurs when the TimedInterruptCustomizerProvider is in play
                    final String errorMessage = String.format("A timeout occurred within the script during evaluation of [%s] - consider increasing the limit given to TimedInterruptCustomizerProvider", msg);
                    logger.warn(errorMessage);
                    writeError(context,
                            ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_TIMEOUT)
                                    .statusMessage("Timeout during script evaluation triggered by TimedInterruptCustomizerProvider")
                                    .statusAttributeException(t).create(),
                            serializer);
                } else if (t instanceof TimeoutException) {
                    final String errorMessage = String.format("Script evaluation exceeded the configured threshold for request [%s]", msg);
                    logger.warn(errorMessage, t);
                    writeError(context,
                            ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_TIMEOUT)
                                    .statusMessage(t.getMessage())
                                    .statusAttributeException(t).create(),
                            serializer);
                } else if (t instanceof MultipleCompilationErrorsException && t.getMessage().contains("Method too large") &&
                        ((MultipleCompilationErrorsException) t).getErrorCollector().getErrorCount() == 1) {
                    final String errorMessage = String.format("The Gremlin statement that was submitted exceeds the maximum compilation size allowed by the JVM, please split it into multiple smaller statements - %s", trimMessage(msg));
                    logger.warn(errorMessage);
                    writeError(context,
                            ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_EVALUATION)
                                    .statusMessage(errorMessage)
                                    .statusAttributeException(t).create(),
                            serializer);
                } else {
                    logger.warn(String.format("Exception processing a Traversal on iteration for request [%s].", msg.getRequestId()), ex);
                    writeError(context,
                            ResponseMessage.build(msg)
                                    .code(ResponseStatusCode.SERVER_ERROR)
                                    .statusMessage(ex.getMessage())
                                    .statusAttributeException(ex)
                                    .create(),
                            serializer);
                }
            }
        } catch (Throwable t) {
            logger.warn(String.format("Exception processing a Traversal on request [%s].", msg.getRequestId()), t);
            writeError(context,
                    ResponseMessage.build(msg)
                            .code(ResponseStatusCode.SERVER_ERROR)
                            .statusMessage(t.getMessage())
                            .statusAttributeException(t)
                            .create(),
                    serializer);
            if (t instanceof Error) {
                //Re-throw any errors to be handled by and set as the result of evalFuture
                throw t;
            }
        }
    }

    private void handleIterator(final Context context, final Iterator itty, final MessageSerializer<?> serializer) throws InterruptedException {
        final ChannelHandlerContext nettyContext = context.getChannelHandlerContext();
        final RequestMessage msg = context.getRequestMessage();
        final Settings settings = context.getSettings();
        boolean warnOnce = false;

        // we have an empty iterator - happens on stuff like: g.V().iterate()
        if (!itty.hasNext()) {
            ByteBuf chunk = null;
            try {
                chunk = makeChunk(context, msg, serializer, new ArrayList<>(), false);
                nettyContext.writeAndFlush(new DefaultHttpContent(chunk));
            } catch (Exception ex) {
                // Bytebuf is a countable release - if it does not get written downstream
                // it needs to be released here
                if (chunk != null) chunk.release();
            }
            sendTrailingHeaders(nettyContext, ResponseStatusCode.SUCCESS, "OK");
            return;
        }

        // the batch size can be overridden by the request
        final int resultIterationBatchSize = (Integer) msg.optionalArgs(Tokens.ARGS_BATCH_SIZE)
                .orElse(settings.resultIterationBatchSize);
        List<Object> aggregate = new ArrayList<>(resultIterationBatchSize);

        // use an external control to manage the loop as opposed to just checking hasNext() in the while.  this
        // prevent situations where auto transactions create a new transaction after calls to commit() withing
        // the loop on calls to hasNext().
        boolean hasMore = itty.hasNext();

        while (hasMore) {
            if (Thread.interrupted()) throw new InterruptedException();

            // have to check the aggregate size because it is possible that the channel is not writeable (below)
            // so iterating next() if the message is not written and flushed would bump the aggregate size beyond
            // the expected resultIterationBatchSize.  Total serialization time for the response remains in
            // effect so if the client is "slow" it may simply timeout.
            //
            // there is a need to check hasNext() on the iterator because if the channel is not writeable the
            // previous pass through the while loop will have next()'d the iterator and if it is "done" then a
            // NoSuchElementException will raise its head. also need a check to ensure that this iteration doesn't
            // require a forced flush which can be forced by sub-classes.
            //
            // this could be placed inside the isWriteable() portion of the if-then below but it seems better to
            // allow iteration to continue into a batch if that is possible rather than just doing nothing at all
            // while waiting for the client to catch up
            if (aggregate.size() < resultIterationBatchSize && itty.hasNext()) aggregate.add(itty.next());

            // Don't keep executor busy if client has already given up; there is no way to catch up if the channel is
            // not active, and hence we should break the loop.
            if (!nettyContext.channel().isActive()) {
                break;
            }

            // send back a page of results if batch size is met or if it's the end of the results being iterated.
            // also check writeability of the channel to prevent OOME for slow clients.
            //
            // clients might decide to close the Netty channel to the server with a CloseWebsocketFrame after errors
            // like CorruptedFrameException. On the server, although the channel gets closed, there might be some
            // executor threads waiting for watermark to clear which will not clear in these cases since client has
            // already given up on these requests. This leads to these executors waiting for the client to consume
            // results till the timeout. checking for isActive() should help prevent that.
            if (nettyContext.channel().isActive() && nettyContext.channel().isWritable()) {
                if (aggregate.size() == resultIterationBatchSize || !itty.hasNext()) {
                    ByteBuf chunk = null;
                    try {
                        chunk = makeChunk(context, msg, serializer, aggregate, itty.hasNext());
                    } catch (Exception ex) {
                        // Bytebuf is a countable release - if it does not get written downstream
                        // it needs to be released here
                        if (chunk != null) chunk.release();

                        // exception is handled in makeFrame() - serialization error gets written back to driver
                        // at that point
                        break;
                    }

                    // track whether there is anything left in the iterator because it needs to be accessed after
                    // the transaction could be closed - in that case a call to hasNext() could open a new transaction
                    // unintentionally
                    hasMore = itty.hasNext();

                    try {
                        // only need to reset the aggregation list if there's more stuff to write
                        if (hasMore) {
                            aggregate = new ArrayList<>(resultIterationBatchSize);
                        }
                    } catch (Exception ex) {
                        // Bytebuf is a countable release - if it does not get written downstream
                        // it needs to be released here
                        if (chunk != null) chunk.release();
                        throw ex;
                    }

                    nettyContext.writeAndFlush(new DefaultHttpContent(chunk));

                    if (!hasMore) {
                        sendTrailingHeaders(nettyContext, ResponseStatusCode.SUCCESS, "OK");
                    }
                }
            } else {
                // don't keep triggering this warning over and over again for the same request
                if (!warnOnce) {
                    logger.warn("Pausing response writing as writeBufferHighWaterMark exceeded on {} - writing will continue once client has caught up", msg);
                    warnOnce = true;
                }

                // since the client is lagging we can hold here for a period of time for the client to catch up.
                // this isn't blocking the IO thread - just a worker.
                TimeUnit.MILLISECONDS.sleep(10);
            }
        }
    }

    /**
     * Check if any exception in the chain is {@link TemporaryException} or {@link Failure} then respond with the
     * right error code so that the client knows to retry.
     */
    private Optional<Throwable> determineIfSpecialException(final Throwable ex) {
        return Stream.of(ExceptionUtils.getThrowables(ex)).
                filter(i -> i instanceof TemporaryException || i instanceof Failure).findFirst();
    }

    private static ByteBuf makeChunk(final Context ctx, final RequestMessage msg,
                                     final MessageSerializer<?> serializer, final List<Object> aggregate,
                                     final boolean hasMore) throws Exception {
        try {
            final ChannelHandlerContext nettyContext = ctx.getChannelHandlerContext();

            ctx.handleDetachment(aggregate);

            if (!hasMore && ctx.getRequestState() == STREAMING) {
                ctx.setRequestState(FINISHING);
            }

            ResponseMessage responseMessage = null;

            // for this state no need to build full ResponseMessage
            if (ctx.getRequestState() != STREAMING) {
                final ResponseMessage.Builder builder = ResponseMessage.buildV4(msg.getRequestId()).result(aggregate);

                // need to put status in last message
                if (ctx.getRequestState() == FINISHING || ctx.getRequestState() == CHUNKING_NOT_SUPPORTED) {
                    builder.code(ResponseStatusCode.SUCCESS).statusMessage("OK");
                }

                responseMessage = builder.create();
            }

            if (ctx.getRequestState() == CHUNKING_NOT_SUPPORTED) {
                return serializer.serializeResponseAsBinary(responseMessage, nettyContext.alloc());
            }

            final MessageChunkSerializer<?> chunkSerializer = (MessageChunkSerializer) serializer;

            switch (ctx.getRequestState()) {
                case NOT_STARTED:
                    if (hasMore) {
                        ctx.setRequestState(STREAMING);
                        return chunkSerializer.writeHeader(responseMessage, nettyContext.alloc());
                    }
                    ctx.setRequestState(FINISHED);

                    return serializer.serializeResponseAsBinary(ResponseMessage.buildV4(msg.getRequestId())
                            .result(aggregate)
                            .code(ResponseStatusCode.SUCCESS)
                            .statusMessage("OK")
                            .create(), nettyContext.alloc());

                case STREAMING:
                    return chunkSerializer.writeChunk(aggregate, nettyContext.alloc());
                case FINISHING:
                    ctx.setRequestState(FINISHED);
                    return chunkSerializer.writeFooter(responseMessage, nettyContext.alloc());
            }

            // todo: just throw?
            return serializer.serializeResponseAsBinary(responseMessage, nettyContext.alloc());

        } catch (Exception ex) {
            logger.warn("The result [{}] in the request {} could not be serialized and returned.", aggregate, msg.getRequestId(), ex);
            final String errorMessage = String.format("Error during serialization: %s", ExceptionHelper.getMessageFromExceptionOrCause(ex));
            writeError(ctx,
                    ResponseMessage.build(msg.getRequestId())
                            .statusMessage(errorMessage)
                            .statusAttributeException(ex)
                            .code(ResponseStatusCode.SERVER_ERROR_SERIALIZATION).create(),
                    serializer);
            throw ex;
        }
    }

    /**
     * Used to decrease the size of a Gremlin script that triggered a "method too large" exception so that it
     * doesn't log a massive text string nor return a large error message.
     */
    private RequestMessage trimMessage(final RequestMessage msg) {
        final RequestMessage trimmedMsg = RequestMessage.from(msg).create();
        if (trimmedMsg.getArgs().containsKey(Tokens.ARGS_GREMLIN))
            trimmedMsg.getArgs().put(Tokens.ARGS_GREMLIN, trimmedMsg.getArgs().get(Tokens.ARGS_GREMLIN).toString().substring(0, 1021) + "...");

        return trimmedMsg;
    }

    public enum RequestState {
        CHUNKING_NOT_SUPPORTED,
        NOT_STARTED,
        STREAMING,
        // last portion of data
        FINISHING,
        FINISHED,
        ERROR
    }
}
