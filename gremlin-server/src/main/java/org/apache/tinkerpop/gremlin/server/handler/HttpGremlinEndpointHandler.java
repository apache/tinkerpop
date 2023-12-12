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
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpChunkedInput;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.stream.ChunkedInput;
import io.netty.handler.stream.ChunkedStream;
import io.netty.util.ReferenceCountUtil;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptChecker;
import org.apache.tinkerpop.gremlin.jsr223.JavaTranslator;
import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Failure;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.AbstractTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.util.BytecodeHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.op.OpProcessorException;
import org.apache.tinkerpop.gremlin.server.op.traversal.TraversalOpProcessor;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.apache.tinkerpop.gremlin.server.util.TextPlainMessageSerializer;
import org.apache.tinkerpop.gremlin.server.util.TraverserIterator;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.TemporaryException;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceFactory;
import org.apache.tinkerpop.gremlin.util.ExceptionHelper;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.function.FunctionUtils;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.util.ser.MessageTextSerializer;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.lang.reflect.UndeclaredThrowableException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.codahale.metrics.MetricRegistry.name;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONTINUE;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * Handler that processes HTTP requests to the HTTP Gremlin endpoint.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@ChannelHandler.Sharable
public class HttpGremlinEndpointHandler extends ChannelInboundHandlerAdapter {
    private static final Bindings EMPTY_BINDINGS = new SimpleBindings();
    public static final Timer traversalOpTimer = MetricManager.INSTANCE.getTimer(name(GremlinServer.class, "op", "traversal"));

    private static final Logger logger = LoggerFactory.getLogger(HttpGremlinEndpointHandler.class);
    private static final Logger auditLogger = LoggerFactory.getLogger(GremlinServer.AUDIT_LOGGER_NAME);

    private static final Timer evalOpTimer = MetricManager.INSTANCE.getTimer(name(GremlinServer.class, "op", "eval"));

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

    private Pair<String, MessageTextSerializer<?>> reqSerializer;

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

            if ("/favicon.ico".equals(req.uri())) {
                HttpHandlerUtil.sendError(ctx, NOT_FOUND, "Gremlin Server doesn't have a favicon.ico", keepAlive);
                ReferenceCountUtil.release(msg);
                return;
            }

            if (HttpUtil.is100ContinueExpected(req)) {
                ctx.write(new DefaultFullHttpResponse(HTTP_1_1, CONTINUE));
            }

            if (req.method() != GET && req.method() != POST) {
                HttpHandlerUtil.sendError(ctx, METHOD_NOT_ALLOWED, METHOD_NOT_ALLOWED.toString(), keepAlive);
                ReferenceCountUtil.release(msg);
                return;
            }

            final RequestMessage requestMessage;
            try {
                requestMessage = HttpHandlerUtil.getRequestMessageFromHttpRequest(req, serializers);
            } catch (IllegalArgumentException|SerializationException ex) {
                HttpHandlerUtil.sendError(ctx, BAD_REQUEST, ex.getMessage(), keepAlive);
                ReferenceCountUtil.release(msg);
                return;
            }

            final String acceptMime = Optional.ofNullable(req.headers().get(HttpHeaderNames.ACCEPT)).orElse("application/json");
            reqSerializer = chooseSerializer(acceptMime);
            if (null == reqSerializer) {
                HttpHandlerUtil.sendError(ctx, BAD_REQUEST, String.format("no serializer for requested Accept header: %s", acceptMime),
                        keepAlive);
                ReferenceCountUtil.release(msg);
                return;
            }

            final String origin = req.headers().get(HttpHeaderNames.ORIGIN);

            // not using the req anywhere below here - assume it is safe to release at this point.
            ReferenceCountUtil.release(msg);

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
//                final ChannelPromise promise = ctx.channel().newPromise();
//                final AtomicReference<Object> resultHolder = new AtomicReference<>();
//                promise.addListener(future -> {
//                    // if failed then the error was already written back to the client as part of the eval future
//                    // processing of the exception
//                    if (future.isSuccess()) {
//                        logger.debug("Preparing HTTP response for request with script [{}] and bindings of [{}] with result of [{}] on [{}]",
//                                requestMessage.getArgOrDefault(Tokens.ARGS_GREMLIN, ""),
//                                requestMessage.getArgOrDefault(Tokens.ARGS_BINDINGS, Collections.emptyMap()),
//                                resultHolder.get(), Thread.currentThread().getName());
//                        final FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK, (ByteBuf) resultHolder.get());
//                        response.headers().set(HttpHeaderNames.CONTENT_TYPE, serializer.getValue0());
//
//                        // handle cors business
//                        if (origin != null) response.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, origin);
//
//                        HttpHandlerUtil.sendAndCleanupConnection(ctx, keepAlive, response);
//                    }
//                });

                final Timer.Context timerContext = evalOpTimer.time();
                Context serverCtx = new Context(requestMessage, ctx, settings, graphManager, gremlinExecutor, gremlinExecutor.getScheduledExecutorService());
                iterateBytecodeTraversal(serverCtx);
//                final Bindings bindings;
//                try {
//                    bindings = createBindings(requestMessage.getArgOrDefault(Tokens.ARGS_BINDINGS, Collections.emptyMap()),
//                            requestMessage.getArgOrDefault(Tokens.ARGS_ALIASES, Collections.emptyMap()));
//                } catch (IllegalStateException iae) {
//                    HttpHandlerUtil.sendError(ctx, BAD_REQUEST, iae.getMessage(), keepAlive);
//                    ReferenceCountUtil.release(msg);
//                    return;
//                }
//
//                // provide a transform function to serialize to message - this will force serialization to occur
//                // in the same thread as the eval. after the CompletableFuture is returned from the eval the result
//                // is ready to be written as a ByteBuf directly to the response.  nothing should be blocking here.
//                final CompletableFuture<Object> evalFuture = gremlinExecutor.eval(
//                        requestMessage.getArg(Tokens.ARGS_GREMLIN), requestMessage.getArg(Tokens.ARGS_LANGUAGE), bindings,
//                        requestMessage.getArgOrDefault(Tokens.ARGS_EVAL_TIMEOUT, null),
//                        FunctionUtils.wrapFunction(o -> {
//                            // stopping the timer here is roughly equivalent to where the timer would have been stopped for
//                            // this metric in other contexts.  we just want to measure eval time not serialization time.
//                            timerContext.stop();
//
//                            logger.debug("Transforming result of request with script [{}] and bindings of [{}] with result of [{}] on [{}]",
//                                    requestMessage.getArg(Tokens.ARGS_GREMLIN),
//                                    requestMessage.getArg(Tokens.ARGS_BINDINGS), o, Thread.currentThread().getName());
//
//                            final Optional<String> mp = requestMessage.getArg(Tokens.ARGS_GREMLIN) instanceof String
//                                    ? GremlinScriptChecker.parse(requestMessage.getArg(Tokens.ARGS_GREMLIN)).getMaterializeProperties()
//                                    : Optional.empty();
//
//                            // need to replicate what TraversalOpProcessor does with the bytecode op. it converts
//                            // results to Traverser so that GLVs can handle the results. don't quite get the same
//                            // benefit here because the bulk has to be 1 since we've already resolved the result,
//                            // but at least http is compatible
//                            final List<Object> results = requestMessage.getOp().equals(Tokens.OPS_BYTECODE) ?
//                                    (List<Object>) IteratorUtils.asList(o).stream().map(r -> new DefaultRemoteTraverser<Object>(r, 1)).collect(Collectors.toList()) :
//                                    IteratorUtils.asList(o);
//
//                            if (mp.isPresent() && mp.get().equals(Tokens.MATERIALIZE_PROPERTIES_TOKENS)) {
//                                final Object firstElement = results.get(0);
//
//                                if (firstElement instanceof Element) {
//                                    for (int i = 0; i < results.size(); i++)
//                                        results.set(i, ReferenceFactory.detach((Element) results.get(i)));
//                                } else if (firstElement instanceof AbstractTraverser) {
//                                    for (final Object item : results)
//                                        ((AbstractTraverser) item).detach();
//                                }
//                            }
//
//                            final ResponseMessage responseMessage = ResponseMessage.build(requestMessage.getRequestId())
//                                    .code(ResponseStatusCode.SUCCESS)
//                                    .result(results).create();
//
//                            // http server is sessionless and must handle commit on transactions. the commit occurs
//                            // before serialization to be consistent with how things work for websocket based
//                            // communication.  this means that failed serialization does not mean that you won't get
//                            // a commit to the database
//                            attemptCommit(requestMessage.getArg(Tokens.ARGS_ALIASES), graphManager, settings.strictTransactionManagement);
//
//                            try {
//                                return Unpooled.wrappedBuffer(serializer.getValue1().serializeResponseAsBinary(responseMessage, ctx.alloc()));
//                            } catch (Exception ex) {
//                                logger.warn(String.format("Error during serialization for %s", responseMessage), ex);
//
//                                // creating a new SerializationException will clear the cause which will allow the
//                                // future to report a better error message. if the cause is present, then
//                                // GremlinExecutor will prefer the cause and we'll get a low level Jackson sort of
//                                // error in the response.
//                                if (ex instanceof SerializationException) {
//                                    throw new SerializationException(String.format(
//                                            "Could not serialize the result with %s - %s",
//                                            serializer.getValue0(),
//                                            ex.getMessage()));
//                                }
//
//                                throw ex;
//                            }
//                        }));
//
//                evalFuture.exceptionally(t -> {
//                    if (t.getMessage() != null)
//                        HttpHandlerUtil.sendError(ctx, INTERNAL_SERVER_ERROR, t.getMessage(), Optional.of(t), keepAlive);
//                    else
//                        HttpHandlerUtil.sendError(ctx, INTERNAL_SERVER_ERROR, String.format("Error encountered evaluating script: %s",
//                                        requestMessage.getArg(Tokens.ARGS_GREMLIN))
//                                , Optional.of(t), keepAlive);
//                    promise.setFailure(t);
//                    return null;
//                });
//
//                evalFuture.thenAcceptAsync(r -> {
//                    // now that the eval/serialization is done in the same thread - complete the promise so we can
//                    // write back the HTTP response on the same thread as the original request
//                    resultHolder.set(r);
//                    promise.setSuccess();
//                }, gremlinExecutor.getExecutorService());
            } catch (Exception ex) {
                // send the error response here and don't rely on exception caught because it might not have the
                // context on whether to close the connection or not, based on keepalive.
                final Throwable t = ExceptionHelper.getRootCause(ex);
                if (t instanceof TooLongFrameException) {
                    HttpHandlerUtil.sendError(ctx, HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, t.getMessage() + " - increase the maxContentLength", keepAlive);
                } else if (t != null){
                    HttpHandlerUtil.sendError(ctx, INTERNAL_SERVER_ERROR, t.getMessage(), keepAlive);
                } else {
                    HttpHandlerUtil.sendError(ctx, INTERNAL_SERVER_ERROR, ex.getMessage(), keepAlive);
                }
            }
        }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
        logger.error("Error processing HTTP Request", cause);

        if (ctx.channel().isActive()) {
            HttpHandlerUtil.sendError(ctx, INTERNAL_SERVER_ERROR, cause.getMessage(), false);
        }
    }

    private Bindings createBindings(final Map<String, Object> bindingMap, final Map<String, String> rebindingMap) {
        final Bindings bindings = new SimpleBindings();

        // rebind any global bindings to a different variable.
        if (!rebindingMap.isEmpty()) {
            for (Map.Entry<String, String> kv : rebindingMap.entrySet()) {
                boolean found = false;
                final Graph g = this.graphManager.getGraph(kv.getValue());
                if (null != g) {
                    bindings.put(kv.getKey(), g);
                    found = true;
                }

                if (!found) {
                    final TraversalSource ts = this.graphManager.getTraversalSource(kv.getValue());
                    if (null != ts) {
                        bindings.put(kv.getKey(), ts);
                        found = true;
                    }
                }

                if (!found) {
                    final String error = String.format("Could not rebind [%s] to [%s] as [%s] not in the Graph or TraversalSource global bindings",
                            kv.getKey(), kv.getValue(), kv.getValue());
                    throw new IllegalStateException(error);
                }
            }
        }

        bindings.putAll(bindingMap);

        return bindings;
    }

    private Pair<String, MessageTextSerializer<?>> chooseSerializer(final String mimeType) {
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
                return Pair.with(accept, (MessageTextSerializer<?>) serializers.get(accept));
            else if (accept.equals("text/plain")) {
                return Pair.with(accept, textPlainSerializer);
            }
        }

        return null;
    }

    private static void attemptCommit(final Map<String, String> aliases, final GraphManager graphManager, final boolean strict) {
        if (strict)
            graphManager.commit(new HashSet<>(aliases.values()));
        else
            graphManager.commitAll();
    }

    private static void validateTraversalSourceAlias(final Context ctx, final RequestMessage message, final Map<String, String> aliases) throws OpProcessorException {
        final String traversalSourceBindingForAlias = aliases.values().iterator().next();
        if (!ctx.getGraphManager().getTraversalSourceNames().contains(traversalSourceBindingForAlias)) {
            final String msg = String.format("The traversal source [%s] for alias [%s] is not configured on the server.", traversalSourceBindingForAlias, Tokens.VAL_TRAVERSAL_SOURCE_ALIAS);
            throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
        }
    }

    private static Map<String, String> validateTraversalRequest(final RequestMessage message) throws OpProcessorException {
        if (!message.optionalArgs(Tokens.ARGS_GREMLIN).isPresent()) {
            final String msg = String.format("A message with [%s] op code requires a [%s] argument.", Tokens.OPS_BYTECODE, Tokens.ARGS_GREMLIN);
            throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
        }

        // matches functionality in the UnifiedHandler
        if (!(message.optionalArgs(Tokens.ARGS_GREMLIN).get() instanceof Bytecode)) {
            final String msg = String.format("A message with [%s] op code requires a [%s] argument that is of type %s.",
                    Tokens.OPS_BYTECODE, Tokens.ARGS_GREMLIN, Bytecode.class.getSimpleName());
            throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
        }

        return validatedAliases(message).get();
    }

    private static Optional<Map<String, String>> validatedAliases(final RequestMessage message) throws OpProcessorException {
        final Optional<Map<String, String>> aliases = message.optionalArgs(Tokens.ARGS_ALIASES);
        if (!aliases.isPresent()) {
            final String msg = String.format("A message with [%s] op code requires a [%s] argument.", Tokens.OPS_BYTECODE, Tokens.ARGS_ALIASES);
            throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
        }

        if (aliases.get().size() != 1 || !aliases.get().containsKey(Tokens.VAL_TRAVERSAL_SOURCE_ALIAS)) {
            final String msg = String.format("A message with [%s] op code requires the [%s] argument to be a Map containing one alias assignment named '%s'.",
                    Tokens.OPS_BYTECODE, Tokens.ARGS_ALIASES, Tokens.VAL_TRAVERSAL_SOURCE_ALIAS);
            throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
        }

        return aliases;
    }

    private void iterateBytecodeTraversal(final Context context) throws Exception {
        final RequestMessage msg = context.getRequestMessage();
        final Settings settings = context.getSettings();
        logger.debug("Traversal request {} for in thread {}", msg.getRequestId(), Thread.currentThread().getName());

        // validateTraversalRequest() ensures that this is of type Bytecode
        final Object bytecodeObj = msg.getArgs().get(Tokens.ARGS_GREMLIN);
        final Bytecode bytecode = (Bytecode) bytecodeObj;

        // earlier validation in selection of this op method should free us to cast this without worry
        final Map<String, String> aliases = (Map<String, String>) msg.optionalArgs(Tokens.ARGS_ALIASES).get();

        // timeout override - handle both deprecated and newly named configuration. earlier logic should prevent
        // both configurations from being submitted at the same time
        final Map<String, Object> args = msg.getArgs();
        final long seto = args.containsKey(Tokens.ARGS_EVAL_TIMEOUT) ?
                ((Number) args.get(Tokens.ARGS_EVAL_TIMEOUT)).longValue() : context.getSettings().getEvaluationTimeout();

        final GraphManager graphManager = context.getGraphManager();
        final String traversalSourceName = aliases.entrySet().iterator().next().getValue();
        final TraversalSource g = graphManager.getTraversalSource(traversalSourceName);

        final Traversal.Admin<?, ?> traversal;
        try {
            final Optional<String> lambdaLanguage = BytecodeHelper.getLambdaLanguage(bytecode);
            if (!lambdaLanguage.isPresent())
                traversal = JavaTranslator.of(g).translate(bytecode);
            else
                traversal = context.getGremlinExecutor().eval(bytecode, EMPTY_BINDINGS, lambdaLanguage.get(), traversalSourceName);
        } catch (ScriptException ex) {
            logger.error("Traversal contains a lambda that cannot be compiled", ex);
            throw new OpProcessorException("Traversal contains a lambda that cannot be compiled",
                    ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_EVALUATION)
                            .statusMessage(ex.getMessage())
                            .statusAttributeException(ex).create());
        } catch (Exception ex) {
            logger.error("Could not deserialize the Traversal instance", ex);
            throw new OpProcessorException("Could not deserialize the Traversal instance",
                    ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_SERIALIZATION)
                            .statusMessage(ex.getMessage())
                            .statusAttributeException(ex).create());
        }
        if (settings.enableAuditLog) {
            AuthenticatedUser user = context.getChannelHandlerContext().channel().attr(StateKey.AUTHENTICATED_USER).get();
            if (null == user) {    // This is expected when using the AllowAllAuthenticator
                user = AuthenticatedUser.ANONYMOUS_USER;
            }
            String address = context.getChannelHandlerContext().channel().remoteAddress().toString();
            if (address.startsWith("/") && address.length() > 1) address = address.substring(1);
            auditLogger.info("User {} with address {} requested: {}", user.getName(), address, bytecode);
        }

        final Timer.Context timerContext = traversalOpTimer.time();
        final FutureTask<Void> evalFuture = new FutureTask<>(() -> {
            context.setStartedResponse();
            final Graph graph = g.getGraph();

            try {
                beforeProcessing(graph, context);

                try {
                    // compile the traversal - without it getEndStep() has nothing in it
                    traversal.applyStrategies();
                    handleIterator(context, new TraverserIterator(traversal), graph);
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
                        context.writeAndFlush(specialResponseMsg.create());
                    } else if (t instanceof InterruptedException || t instanceof TraversalInterruptedException) {
                        graphManager.onQueryError(msg, t);
                        final String errorMessage = String.format("A timeout occurred during traversal evaluation of [%s] - consider increasing the limit given to evaluationTimeout", msg);
                        logger.warn(errorMessage);
                        context.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_TIMEOUT)
                                .statusMessage(errorMessage)
                                .statusAttributeException(ex).create());
                    } else {
                        logger.warn(String.format("Exception processing a Traversal on iteration for request [%s].", msg.getRequestId()), ex);
                        context.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR)
                                .statusMessage(ex.getMessage())
                                .statusAttributeException(ex).create());
                    }
                    onError(graph, context, ex);
                }
            } catch (Throwable t) {
                onError(graph, context, t);
                // if any exception in the chain is TemporaryException or Failure then we should respond with the
                // right error code so that the client knows to retry
                final Optional<Throwable> possibleSpecialException = determineIfSpecialException(t);
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
                    context.writeAndFlush(specialResponseMsg.create());
                } else {
                    logger.warn(String.format("Exception processing a Traversal on request [%s].", msg.getRequestId()), t);
                    context.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR)
                            .statusMessage(t.getMessage())
                            .statusAttributeException(t).create());
                    if (t instanceof Error) {
                        //Re-throw any errors to be handled by and set as the result of evalFuture
                        throw t;
                    }
                }
            } finally {
                timerContext.stop();

                // There is a race condition that this query may have finished before the timeoutFuture was created,
                // though this is very unlikely. This is handled in the settor, if this has already been grabbed.
                // If we passed this point and the setter hasn't been called, it will cancel the timeoutFuture inside
                // the setter to compensate.
                final ScheduledFuture<?> timeoutFuture = context.getTimeoutExecutor();
                if (null != timeoutFuture)
                    timeoutFuture.cancel(true);
            }

            return null;
        });

        try {
            final Future<?> executionFuture = context.getGremlinExecutor().getExecutorService().submit(evalFuture);
            if (seto > 0) {
                // Schedule a timeout in the thread pool for future execution
                context.setTimeoutExecutor(context.getScheduledExecutorService().schedule(() -> {
                    executionFuture.cancel(true);
                    if (!context.getStartedResponse()) {
                        context.sendTimeoutResponse();
                    }
                }, seto, TimeUnit.MILLISECONDS));
            }
        } catch (RejectedExecutionException ree) {
            context.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.TOO_MANY_REQUESTS)
                    .statusMessage("Rate limiting").create());
        }
    }

    protected void beforeProcessing(final Graph graph, final Context ctx) {
        final GraphManager graphManager = ctx.getGraphManager();
        final RequestMessage msg = ctx.getRequestMessage();
        graphManager.beforeQueryStart(msg);
        if (graph.features().graph().supportsTransactions() && graph.tx().isOpen()) graph.tx().rollback();
    }

    protected void onError(final Graph graph, final Context ctx, Throwable error) {
        final GraphManager graphManager = ctx.getGraphManager();
        final RequestMessage msg = ctx.getRequestMessage();
        graphManager.onQueryError(msg, error);
        if (graph.features().graph().supportsTransactions() && graph.tx().isOpen()) graph.tx().rollback();
    }

    protected void onTraversalSuccess(final Graph graph, final Context ctx) {
        final GraphManager graphManager = ctx.getGraphManager();
        final RequestMessage msg = ctx.getRequestMessage();
        graphManager.onQuerySuccess(msg);
        if (graph.features().graph().supportsTransactions() && graph.tx().isOpen()) graph.tx().commit();
    }

    protected void handleIterator(final Context context, final Iterator itty, final Graph graph) throws InterruptedException {
        final ChannelHandlerContext nettyContext = context.getChannelHandlerContext();
        final RequestMessage msg = context.getRequestMessage();
        final Settings settings = context.getSettings();
//        final MessageSerializer<?> serializer = nettyContext.channel().attr(StateKey.SERIALIZER).get(); get from member variable
        final boolean useBinary = true; // nettyContext.channel().attr(StateKey.USE_BINARY).get();
        boolean warnOnce = false;

        // we have an empty iterator - happens on stuff like: g.V().iterate()
        if (!itty.hasNext()) {
            final Map<String, Object> attributes = generateStatusAttributes(nettyContext, msg, ResponseStatusCode.NO_CONTENT, itty, settings);

            // as there is nothing left to iterate if we are transaction managed then we should execute a
            // commit here before we send back a NO_CONTENT which implies success
            onTraversalSuccess(graph, context);
            context.writeAndFlush(ResponseMessage.build(msg)
                    .code(ResponseStatusCode.NO_CONTENT)
                    .statusAttributes(attributes)
                    .create());
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

        final HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, reqSerializer.getValue0());
        HttpUtil.setTransferEncodingChunked(response, true);
        nettyContext.write(response);

        while (hasMore) {
            if (Thread.interrupted()) throw new InterruptedException();

            // check if an implementation needs to force flush the aggregated results before the iteration batch
            // size is reached.
            final boolean forceFlush = isForceFlushed(nettyContext, msg, itty);

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
            if (aggregate.size() < resultIterationBatchSize && itty.hasNext() && !forceFlush) aggregate.add(itty.next());

            // Don't keep executor busy if client has already given up; there is no way to catch up if the channel is
            // not active, and hence we should break the loop.
            if (!nettyContext.channel().isActive()) {
                onError(graph, context, new ChannelException("Channel is not active - cannot write any more results"));
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
                if (forceFlush || aggregate.size() == resultIterationBatchSize || !itty.hasNext()) {
                    final ResponseStatusCode code = itty.hasNext() ? ResponseStatusCode.PARTIAL_CONTENT : ResponseStatusCode.SUCCESS;

                    // serialize here because in sessionless requests the serialization must occur in the same
                    // thread as the eval.  as eval occurs in the GremlinExecutor there's no way to get back to the
                    // thread that processed the eval of the script so, we have to push serialization down into that
                    final Map<String, Object> metadata = generateResultMetaData(nettyContext, msg, code, itty, settings);
                    final Map<String, Object> statusAttrb = generateStatusAttributes(nettyContext, msg, code, itty, settings);
                    Frame frame = null;
                    try {
                        frame = makeFrame(context, msg, reqSerializer.getValue1(), useBinary, aggregate, code,
                                metadata, statusAttrb);
                    } catch (Exception ex) {
                        // a frame may use a Bytebuf which is a countable release - if it does not get written
                        // downstream it needs to be released here
                        if (frame != null) frame.tryRelease();

                        // exception is handled in makeFrame() - serialization error gets written back to driver
                        // at that point
                        onError(graph, context, ex);
                        break;
                    }

                    // track whether there is anything left in the iterator because it needs to be accessed after
                    // the transaction could be closed - in that case a call to hasNext() could open a new transaction
                    // unintentionally
                    hasMore = itty.hasNext();

                    try {
                        // only need to reset the aggregation list if there's more stuff to write
                        if (hasMore)
                            aggregate = new ArrayList<>(resultIterationBatchSize);
                        else {
                            // iteration and serialization are both complete which means this finished successfully. note that
                            // errors internal to script eval or timeout will rollback given GremlinServer's global configurations.
                            // local errors will get rolled back below because the exceptions aren't thrown in those cases to be
                            // caught by the GremlinExecutor for global rollback logic. this only needs to be committed if
                            // there are no more items to iterate and serialization is complete
                            onTraversalSuccess(graph, context);
                        }
                    } catch (Exception ex) {
                        // a frame may use a Bytebuf which is a countable release - if it does not get written
                        // downstream it needs to be released here
                        if (frame != null) frame.tryRelease();
                        throw ex;
                    }

                    if (!hasMore) iterateComplete(nettyContext, msg, itty);

                    // the flush is called after the commit has potentially occurred.  in this way, if a commit was
                    // required then it will be 100% complete before the client receives it. the "frame" at this point
                    // should have completely detached objects from the transaction (i.e. serialization has occurred)
                    // so a new one should not be opened on the flush down the netty pipeline
//                    don't do this for the first part as we need to return http response not ws
//                    context.writeAndFlush(code, frame);
//                    final FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK, (ByteBuf) frame.getMsg());
//                    response.headers().set(HttpHeaderNames.CONTENT_TYPE, reqSerializer.getValue0());

                    // handle cors business
//                    if (origin != null) response.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, origin);


                    if (hasMore) {
                        nettyContext.writeAndFlush(new PartialChunkedInput(new ChunkedStream(new ByteBufInputStream((ByteBuf) frame.getMsg()))));
                    } else {
                        nettyContext.writeAndFlush(new HttpChunkedInput(new ChunkedStream(new ByteBufInputStream((ByteBuf) frame.getMsg()))));
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

    protected static Optional<Throwable> determineIfSpecialException(final Throwable ex) {
        return Stream.of(ExceptionUtils.getThrowables(ex)).
                filter(i -> i instanceof TemporaryException || i instanceof Failure).findFirst();
    }

    protected void iterateComplete(final ChannelHandlerContext ctx, final RequestMessage msg, final Iterator itty) {
        // do nothing by default
    }

    protected Map<String, Object> generateResultMetaData(final ChannelHandlerContext ctx, final RequestMessage msg,
                                                         final ResponseStatusCode code, final Iterator itty,
                                                         final Settings settings) {
        return Collections.emptyMap();
    }

    /**
     * Generates response status meta-data to put on a {@link ResponseMessage}.
     *
     * @param itty a reference to the current {@link Iterator} of results - it is not meant to be forwarded in
     *             this method
     */
    protected Map<String, Object> generateStatusAttributes(final ChannelHandlerContext ctx, final RequestMessage msg,
                                                           final ResponseStatusCode code, final Iterator itty,
                                                           final Settings settings) {
        // only return server metadata on the last message
        if (itty.hasNext()) return Collections.emptyMap();

        final Map<String, Object> metaData = new HashMap<>();
        metaData.put(Tokens.ARGS_HOST, ctx.channel().remoteAddress().toString());

        return metaData;
    }

    protected static Frame makeFrame(final Context ctx, final RequestMessage msg,
                                     final MessageSerializer<?> serializer, final boolean useBinary, final List<Object> aggregate,
                                     final ResponseStatusCode code, final Map<String,Object> responseMetaData,
                                     final Map<String,Object> statusAttributes) throws Exception {
        try {
            final ChannelHandlerContext nettyContext = ctx.getChannelHandlerContext();

            ctx.handleDetachment(aggregate);

            if (useBinary) {
                return new Frame(serializer.serializeResponseAsBinary(ResponseMessage.build(msg)
                        .code(code)
                        .statusAttributes(statusAttributes)
                        .responseMetaData(responseMetaData)
                        .result(aggregate).create(), nettyContext.alloc()));
            } else {
                // the expectation is that the GremlinTextRequestDecoder will have placed a MessageTextSerializer
                // instance on the channel.
                final MessageTextSerializer<?> textSerializer = (MessageTextSerializer<?>) serializer;
                return new Frame(textSerializer.serializeResponseAsString(ResponseMessage.build(msg)
                        .code(code)
                        .statusAttributes(statusAttributes)
                        .responseMetaData(responseMetaData)
                        .result(aggregate).create(), nettyContext.alloc()));
            }
        } catch (Exception ex) {
            logger.warn("The result [{}] in the request {} could not be serialized and returned.", aggregate, msg.getRequestId(), ex);
            final String errorMessage = String.format("Error during serialization: %s", ExceptionHelper.getMessageFromExceptionOrCause(ex));
            final ResponseMessage error = ResponseMessage.build(msg.getRequestId())
                    .statusMessage(errorMessage)
                    .statusAttributeException(ex)
                    .code(ResponseStatusCode.SERVER_ERROR_SERIALIZATION).create();
            ctx.writeAndFlush(error);
            throw ex;
        }
    }

    protected boolean isForceFlushed(final ChannelHandlerContext ctx, final RequestMessage msg, final Iterator itty) {
        return false;
    }
}
