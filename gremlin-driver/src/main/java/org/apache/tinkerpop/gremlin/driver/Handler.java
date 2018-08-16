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
package org.apache.tinkerpop.gremlin.driver;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.security.PrivilegedActionException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

/**
 * Holder for internal handler classes used in constructing the channel pipeline.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
final class Handler {

    /**
     * Generic SASL handler that will authenticate against the gremlin server.
     */
    static class GremlinSaslAuthenticationHandler extends SimpleChannelInboundHandler<ResponseMessage> implements CallbackHandler {
        private static final Logger logger = LoggerFactory.getLogger(GremlinSaslAuthenticationHandler.class);
        private static final AttributeKey<Subject> subjectKey = AttributeKey.valueOf("subject");
        private static final AttributeKey<SaslClient> saslClientKey = AttributeKey.valueOf("saslclient");
        private static final Map<String, String> SASL_PROPERTIES = new HashMap<String, String>() {{ put(Sasl.SERVER_AUTH, "true"); }};
        private static final byte[] NULL_CHALLENGE = new byte[0];

        private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();
        private static final Base64.Decoder BASE64_DECODER = Base64.getDecoder();

        private final AuthProperties authProps;

        public GremlinSaslAuthenticationHandler(final AuthProperties authProps) {
            this.authProps = authProps;
        }

        @Override
        protected void channelRead0(final ChannelHandlerContext channelHandlerContext, final ResponseMessage response) throws Exception {
            // We are only interested in AUTHENTICATE responses here. Everything else can
            // get passed down the pipeline
            if (response.getStatus().getCode() == ResponseStatusCode.AUTHENTICATE) {
                final Attribute<SaslClient> saslClient = channelHandlerContext.attr(saslClientKey);
                final Attribute<Subject> subject = channelHandlerContext.attr(subjectKey);
                final RequestMessage.Builder messageBuilder = RequestMessage.build(Tokens.OPS_AUTHENTICATION);
                // First time through we don't have a sasl client
                if (saslClient.get() == null) {
                    subject.set(login());
                    try {
                        saslClient.set(saslClient(getHostName(channelHandlerContext)));
                    } catch (SaslException saslException) {
                        // push the sasl error into a failure response from the server. this ensures that standard
                        // processing for the ResultQueue is kept. without this SaslException trap and subsequent
                        // conversion to an authentication failure, the close() of the connection might not
                        // succeed as it will appear as though pending messages remain present in the queue on the
                        // connection and the shutdown won't proceed
                        final ResponseMessage clientSideError = ResponseMessage.build(response.getRequestId())
                                .code(ResponseStatusCode.FORBIDDEN).statusMessage(saslException.getMessage()).create();
                        channelHandlerContext.fireChannelRead(clientSideError);
                        return;
                    }

                    messageBuilder.addArg(Tokens.ARGS_SASL_MECHANISM, getMechanism());
                    messageBuilder.addArg(Tokens.ARGS_SASL, saslClient.get().hasInitialResponse() ?
                            BASE64_ENCODER.encodeToString(evaluateChallenge(subject, saslClient, NULL_CHALLENGE)) : null);
                } else {
                    // the server sends base64 encoded sasl as well as the byte array. the byte array will eventually be
                    // phased out, but is present now for backward compatibility in 3.2.x
                    final String base64sasl = response.getStatus().getAttributes().containsKey(Tokens.ARGS_SASL) ?
                        response.getStatus().getAttributes().get(Tokens.ARGS_SASL).toString() :
                        BASE64_ENCODER.encodeToString((byte[]) response.getResult().getData());

                    messageBuilder.addArg(Tokens.ARGS_SASL, BASE64_ENCODER.encodeToString(
                        evaluateChallenge(subject, saslClient, BASE64_DECODER.decode(base64sasl))));
                }
                channelHandlerContext.writeAndFlush(messageBuilder.create());
            } else {
                channelHandlerContext.fireChannelRead(response);
            }
        }

        public void handle(final Callback[] callbacks) {
            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback) {
                    if (authProps.get(AuthProperties.Property.USERNAME) != null) {
                        ((NameCallback)callback).setName(authProps.get(AuthProperties.Property.USERNAME));
                    }
                } else if (callback instanceof PasswordCallback) {
                    if (authProps.get(AuthProperties.Property.PASSWORD) != null) {
                        ((PasswordCallback)callback).setPassword(authProps.get(AuthProperties.Property.PASSWORD).toCharArray());
                    }
                } else {
                    logger.warn("SASL handler got a callback of type " + callback.getClass().getCanonicalName());
                }
            }
        }

        private byte[] evaluateChallenge(final Attribute<Subject> subject, final Attribute<SaslClient> saslClient,
                                         final byte[] challenge) throws SaslException {

            if (subject.get() == null) {
                return saslClient.get().evaluateChallenge(challenge);
            } else {
                // If we have a subject then run this as a privileged action using the subject
                try {
                    return Subject.doAs(subject.get(), (PrivilegedExceptionAction<byte[]>) () -> saslClient.get().evaluateChallenge(challenge));
                } catch (PrivilegedActionException e) {
                    throw (SaslException)e.getException();
                }
            }
        }

        private Subject login() throws LoginException {
            // Login if the user provided us with an entry into the JAAS config file
            if (authProps.get(AuthProperties.Property.JAAS_ENTRY) != null) {
                final LoginContext login = new LoginContext(authProps.get(AuthProperties.Property.JAAS_ENTRY));
                login.login();
                return login.getSubject();
            }
            return null;
        }

        private SaslClient saslClient(final String hostname) throws SaslException {
            return Sasl.createSaslClient(new String[] { getMechanism() }, null, authProps.get(AuthProperties.Property.PROTOCOL),
                                         hostname, SASL_PROPERTIES, this);
        }

        private String getHostName(final ChannelHandlerContext channelHandlerContext) {
            return ((InetSocketAddress)channelHandlerContext.channel().remoteAddress()).getAddress().getCanonicalHostName();
        }

        /**
         * Work out the Sasl mechanism based on the user supplied parameters.
         * If we have a username and password use PLAIN otherwise GSSAPI
         * ToDo: have gremlin-server provide the mechanism(s) it is configured with, so that additional mechanisms can
         * be supported in the driver and confusing GSSException messages from the driver are avoided
         */
        private String getMechanism() {
            if ((authProps.get(AuthProperties.Property.USERNAME) != null) &&
                (authProps.get(AuthProperties.Property.PASSWORD) != null)) {
                return "PLAIN";
            } else {
                return "GSSAPI";
            }
        }
    }

    /**
     * Takes a map of requests pending responses and writes responses to the {@link ResultQueue} of a request
     * as the {@link ResponseMessage} objects are deserialized.
     */
    static class GremlinResponseHandler extends SimpleChannelInboundHandler<ResponseMessage> {
        private static final Logger logger = LoggerFactory.getLogger(GremlinResponseHandler.class);
        private final ConcurrentMap<UUID, ResultQueue> pending;

        public GremlinResponseHandler(final ConcurrentMap<UUID, ResultQueue> pending) {
            this.pending = pending;
        }

        @Override
        public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
            // occurs when the server shutsdown in a disorderly fashion, otherwise in an orderly shutdown the server
            // should fire off a close message which will properly release the driver.
            super.channelInactive(ctx);

            // the channel isn't going to get anymore results as it is closed so release all pending requests
            pending.values().forEach(val -> val.markError(new IllegalStateException("Connection to server is no longer active")));
            pending.clear();
        }

        @Override
        protected void channelRead0(final ChannelHandlerContext channelHandlerContext, final ResponseMessage response) throws Exception {
            try {
                final ResponseStatusCode statusCode = response.getStatus().getCode();
                final ResultQueue queue = pending.get(response.getRequestId());
                if (statusCode == ResponseStatusCode.SUCCESS || statusCode == ResponseStatusCode.PARTIAL_CONTENT) {
                    final Object data = response.getResult().getData();
                    final Map<String,Object> meta = response.getResult().getMeta();

                    if (!meta.containsKey(Tokens.ARGS_SIDE_EFFECT_KEY)) {
                        // this is a "result" from the server which is either the result of a script or a
                        // serialized traversal
                        if (data instanceof List) {
                            // unrolls the collection into individual results to be handled by the queue.
                            final List<Object> listToUnroll = (List<Object>) data;
                            listToUnroll.forEach(item -> queue.add(new Result(item)));
                        } else {
                            // since this is not a list it can just be added to the queue
                            queue.add(new Result(response.getResult().getData()));
                        }
                    } else {
                        // this is the side-effect from the server which is generated from a serialized traversal
                        final String aggregateTo = meta.getOrDefault(Tokens.ARGS_AGGREGATE_TO, Tokens.VAL_AGGREGATE_TO_NONE).toString();
                        if (data instanceof List) {
                            // unrolls the collection into individual results to be handled by the queue.
                            final List<Object> listOfSideEffects = (List<Object>) data;
                            listOfSideEffects.forEach(sideEffect -> queue.addSideEffect(aggregateTo, sideEffect));
                        } else {
                            // since this is not a list it can just be added to the queue. this likely shouldn't occur
                            // however as the protocol will typically push everything to list first.
                            queue.addSideEffect(aggregateTo, data);
                        }
                    }
                } else {
                    // this is a "success" but represents no results otherwise it is an error
                    if (statusCode != ResponseStatusCode.NO_CONTENT) {
                        final Map<String,Object> attributes = response.getStatus().getAttributes();
                        final String stackTrace = attributes.containsKey(Tokens.STATUS_ATTRIBUTE_STACK_TRACE) ?
                                (String) attributes.get(Tokens.STATUS_ATTRIBUTE_STACK_TRACE) : null;
                        final List<String> exceptions = attributes.containsKey(Tokens.STATUS_ATTRIBUTE_EXCEPTIONS) ?
                                (List<String>) attributes.get(Tokens.STATUS_ATTRIBUTE_EXCEPTIONS) : null;
                        queue.markError(new ResponseException(response.getStatus().getCode(), response.getStatus().getMessage(),
                                exceptions, stackTrace, cleanStatusAttributes(attributes)));
                    }
                }

                // the last message in a OK stream could have meta-data that is useful to the result. note that error
                // handling of the status attributes is handled separately above
                if (statusCode == ResponseStatusCode.SUCCESS || statusCode == ResponseStatusCode.NO_CONTENT) {
                    // in 3.4.0 this should get refactored. i think the that the markComplete() could just take the
                    // status attributes as its argument - need to investigate that further
                    queue.statusAttributes = response.getStatus().getAttributes();
                }

                // as this is a non-PARTIAL_CONTENT code - the stream is done.
                if (statusCode != ResponseStatusCode.PARTIAL_CONTENT) {
                    queue.statusAttributes = response.getStatus().getAttributes();
                    pending.remove(response.getRequestId()).markComplete();
                }
            } finally {
                // in the event of an exception above the exception is tossed and handled by whatever channelpipeline
                // error handling is at play.
                ReferenceCountUtil.release(response);
            }
        }

        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
            // if this happens enough times (like the client is unable to deserialize a response) the pending
            // messages queue will not clear.  wonder if there is some way to cope with that.  of course, if
            // there are that many failures someone would take notice and hopefully stop the client.
            logger.error("Could not process the response", cause);

            // the channel took an error because of something pretty bad so release all the futures out there
            pending.values().forEach(val -> val.markError(cause));
            pending.clear();

            // serialization exceptions should not close the channel - that's worth a retry
            if (!IteratorUtils.anyMatch(ExceptionUtils.getThrowableList(cause).iterator(), t -> t instanceof SerializationException))
                if (ctx.channel().isActive()) ctx.close();
        }

        private Map<String,Object> cleanStatusAttributes(final Map<String,Object> statusAttributes) {
            final Map<String,Object> m = new HashMap<>();
            statusAttributes.forEach((k,v) -> {
                if (!k.equals(Tokens.STATUS_ATTRIBUTE_EXCEPTIONS) && !k.equals(Tokens.STATUS_ATTRIBUTE_STACK_TRACE))
                    m.put(k,v);
            });
            return m;
        }
    }

}
