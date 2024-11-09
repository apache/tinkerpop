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
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.util.ReferenceCountUtil;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptChecker;
import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.AbstractTraverser;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.apache.tinkerpop.gremlin.server.util.TextPlainMessageSerializer;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
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
import javax.script.SimpleBindings;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
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

            final UUID requestId = requestMessage.getRequestId();
            final String acceptMime = Optional.ofNullable(req.headers().get(HttpHeaderNames.ACCEPT)).orElse("application/json");
            final Pair<String, MessageTextSerializer<?>> serializer = chooseSerializer(acceptMime);
            if (null == serializer) {
                HttpHandlerUtil.sendError(ctx, BAD_REQUEST, requestId, String.format("no serializer for requested Accept header: %s", acceptMime),
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
                final ChannelPromise promise = ctx.channel().newPromise();
                final AtomicReference<Object> resultHolder = new AtomicReference<>();
                promise.addListener(future -> {
                    // if failed then the error was already written back to the client as part of the eval future
                    // processing of the exception
                    if (future.isSuccess()) {
                        logger.debug("Preparing HTTP response for request with script [{}] and bindings of [{}] with result of [{}] on [{}]",
                                requestMessage.getArgOrDefault(Tokens.ARGS_GREMLIN, ""),
                                requestMessage.getArgOrDefault(Tokens.ARGS_BINDINGS, Collections.emptyMap()),
                                resultHolder.get(), Thread.currentThread().getName());
                        final FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK, (ByteBuf) resultHolder.get());
                        response.headers().set(HttpHeaderNames.CONTENT_TYPE, serializer.getValue0());

                        // handle cors business
                        if (origin != null) response.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, origin);

                        HttpHandlerUtil.sendAndCleanupConnection(ctx, keepAlive, response);
                    }
                });

                final Timer.Context timerContext = evalOpTimer.time();

                final Bindings bindings;
                try {
                    bindings = createBindings(requestMessage.getArgOrDefault(Tokens.ARGS_BINDINGS, Collections.emptyMap()),
                            requestMessage.getArgOrDefault(Tokens.ARGS_ALIASES, Collections.emptyMap()));
                } catch (IllegalStateException iae) {
                    HttpHandlerUtil.sendError(ctx, BAD_REQUEST, requestId, iae.getMessage(), keepAlive);
                    ReferenceCountUtil.release(msg);
                    return;
                }

                // provide a transform function to serialize to message - this will force serialization to occur
                // in the same thread as the eval. after the CompletableFuture is returned from the eval the result
                // is ready to be written as a ByteBuf directly to the response.  nothing should be blocking here.
                final CompletableFuture<Object> evalFuture = gremlinExecutor.eval(
                        requestMessage.getArg(Tokens.ARGS_GREMLIN), requestMessage.getArg(Tokens.ARGS_LANGUAGE), bindings,
                        requestMessage.getArgOrDefault(Tokens.ARGS_EVAL_TIMEOUT, null),
                        FunctionUtils.wrapFunction(o -> {
                            // stopping the timer here is roughly equivalent to where the timer would have been stopped for
                            // this metric in other contexts.  we just want to measure eval time not serialization time.
                            timerContext.stop();

                            logger.debug("Transforming result of request with script [{}] and bindings of [{}] with result of [{}] on [{}]",
                                    requestMessage.getArg(Tokens.ARGS_GREMLIN),
                                    requestMessage.getArg(Tokens.ARGS_BINDINGS), o, Thread.currentThread().getName());

                            final Optional<String> mp = requestMessage.getArg(Tokens.ARGS_GREMLIN) instanceof String
                                    ? GremlinScriptChecker.parse(requestMessage.getArg(Tokens.ARGS_GREMLIN)).getMaterializeProperties()
                                    : Optional.empty();

                            // need to replicate what TraversalOpProcessor does with the bytecode op. it converts
                            // results to Traverser so that GLVs can handle the results. don't quite get the same
                            // benefit here because the bulk has to be 1 since we've already resolved the result,
                            // but at least http is compatible
                            final List<Object> results = requestMessage.getOp().equals(Tokens.OPS_BYTECODE) ?
                                    (List<Object>) IteratorUtils.asList(o).stream().map(r -> new DefaultRemoteTraverser<Object>(r, 1)).collect(Collectors.toList()) :
                                    IteratorUtils.asList(o);

                            if (mp.isPresent() && mp.get().equals(Tokens.MATERIALIZE_PROPERTIES_TOKENS)) {
                                final Object firstElement = results.get(0);

                                if (firstElement instanceof Element) {
                                    for (int i = 0; i < results.size(); i++)
                                        results.set(i, ReferenceFactory.detach((Element) results.get(i)));
                                } else if (firstElement instanceof AbstractTraverser) {
                                    for (final Object item : results)
                                        ((AbstractTraverser) item).detach();
                                }
                            }

                            final ResponseMessage responseMessage = ResponseMessage.build(requestId)
                                    .code(ResponseStatusCode.SUCCESS)
                                    .result(results).create();

                            // http server is sessionless and must handle commit on transactions. the commit occurs
                            // before serialization to be consistent with how things work for websocket based
                            // communication.  this means that failed serialization does not mean that you won't get
                            // a commit to the database
                            attemptCommit(requestMessage.getArg(Tokens.ARGS_ALIASES), graphManager, settings.strictTransactionManagement);

                            try {
                                return Unpooled.wrappedBuffer(serializer.getValue1().serializeResponseAsBinary(responseMessage, ctx.alloc()));
                            } catch (Exception ex) {
                                logger.warn(String.format("Error during serialization for %s", responseMessage), ex);

                                // creating a new SerializationException will clear the cause which will allow the
                                // future to report a better error message. if the cause is present, then
                                // GremlinExecutor will prefer the cause and we'll get a low level Jackson sort of
                                // error in the response.
                                if (ex instanceof SerializationException) {
                                    throw new SerializationException(String.format(
                                            "Could not serialize the result with %s - %s",
                                            serializer.getValue0(),
                                            ex.getMessage()));
                                }

                                throw ex;
                            }
                        }));

                evalFuture.exceptionally(t -> {
                    if (t.getMessage() != null)
                        HttpHandlerUtil.sendError(ctx, INTERNAL_SERVER_ERROR, requestId, t.getMessage(), Optional.of(t), keepAlive);
                    else
                        HttpHandlerUtil.sendError(ctx, INTERNAL_SERVER_ERROR, requestId, String.format("Error encountered evaluating script: %s",
                                        requestMessage.getArg(Tokens.ARGS_GREMLIN))
                                , Optional.of(t), keepAlive);
                    promise.setFailure(t);
                    return null;
                });

                evalFuture.thenAcceptAsync(r -> {
                    // now that the eval/serialization is done in the same thread - complete the promise so we can
                    // write back the HTTP response on the same thread as the original request
                    resultHolder.set(r);
                    promise.setSuccess();
                }, gremlinExecutor.getExecutorService());
            } catch (Exception ex) {
                // send the error response here and don't rely on exception caught because it might not have the
                // context on whether to close the connection or not, based on keepalive.
                final Throwable t = ExceptionHelper.getRootCause(ex);
                if (t instanceof TooLongFrameException) {
                    HttpHandlerUtil.sendError(ctx, HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, requestId, t.getMessage() + " - increase the maxContentLength", keepAlive);
                } else if (t != null){
                    HttpHandlerUtil.sendError(ctx, INTERNAL_SERVER_ERROR, requestId, t.getMessage(), keepAlive);
                } else {
                    HttpHandlerUtil.sendError(ctx, INTERNAL_SERVER_ERROR, requestId, ex.getMessage(), keepAlive);
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
}
