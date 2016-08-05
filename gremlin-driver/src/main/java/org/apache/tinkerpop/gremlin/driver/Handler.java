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
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.security.PrivilegedActionException;
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
                RequestMessage.Builder messageBuilder = RequestMessage.build(Tokens.OPS_AUTHENTICATION);
                // First time through we don't have a sasl client
                if (saslClient.get() == null) {
                    subject.set(login());
                    saslClient.set(saslClient(getHostName(channelHandlerContext)));
                    messageBuilder.addArg(Tokens.ARGS_SASL_MECHANISM, getMechanism());
                    messageBuilder.addArg(Tokens.ARGS_SASL, saslClient.get().hasInitialResponse() ?
                                                                evaluateChallenge(subject, saslClient, NULL_CHALLENGE) : null);
                } else {
                    messageBuilder.addArg(Tokens.ARGS_SASL, evaluateChallenge(subject, saslClient, (byte[])response.getResult().getData()));
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
        private final boolean unrollTraversers;

        public GremlinResponseHandler(final ConcurrentMap<UUID, ResultQueue> pending, final Client.Settings settings) {
            this.pending = pending;
            unrollTraversers = settings.unrollTraversers();
        }

        @Override
        protected void channelRead0(final ChannelHandlerContext channelHandlerContext, final ResponseMessage response) throws Exception {
            try {
                final ResponseStatusCode statusCode = response.getStatus().getCode();
                final ResultQueue queue = pending.get(response.getRequestId());
                if (statusCode == ResponseStatusCode.SUCCESS || statusCode == ResponseStatusCode.PARTIAL_CONTENT) {
                    final Object data = response.getResult().getData();
                    final Map<String,Object> meta = response.getResult().getMeta();

                    if (!meta.containsKey(Tokens.ARGS_SIDE_EFFECT)) {
                        // this is a "result" from the server which is either the result of a script or a
                        // serialized traversal
                        if (data instanceof List) {
                            // unrolls the collection into individual results to be handled by the queue.
                            final List<Object> listToUnroll = (List<Object>) data;
                            listToUnroll.forEach(item -> tryUnrollTraverser(queue, item));
                        } else {
                            // since this is not a list it can just be added to the queue
                            tryUnrollTraverser(queue, response.getResult().getData());
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
                    if (statusCode != ResponseStatusCode.NO_CONTENT)
                        queue.markError(new ResponseException(response.getStatus().getCode(), response.getStatus().getMessage()));
                }

                // as this is a non-PARTIAL_CONTENT code - the stream is done
                if (response.getStatus().getCode() != ResponseStatusCode.PARTIAL_CONTENT)
                    pending.remove(response.getRequestId()).markComplete();
            } finally {
                // in the event of an exception above the exception is tossed and handled by whatever channelpipeline
                // error handling is at play.
                ReferenceCountUtil.release(response);
            }
        }

        private void tryUnrollTraverser(final ResultQueue queue, final Object item) {
            if (unrollTraversers) {
                if (item instanceof Traverser.Admin) {
                    final Traverser.Admin t = (Traverser.Admin) item;
                    final Object result = t.get();
                    for (long ix = 0; ix < t.bulk(); ix++) {
                        queue.add(new Result(result));
                    }
                } else {
                    queue.add(new Result(item));
                }
            } else {
                queue.add(new Result(item));
            }
        }

        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
            // if this happens enough times (like the client is unable to deserialize a response) the pending
            // messages queue will not clear.  wonder if there is some way to cope with that.  of course, if
            // there are that many failures someone would take notice and hopefully stop the client.
            logger.error("Could not process the response", cause);

            // the channel took an error because of something pretty bad so release all the completeable
            // futures out there
            pending.entrySet().stream().forEach(kv -> kv.getValue().markError(cause));

            // serialization exceptions should not close the channel - that's worth a retry
            if (!ExceptionUtils.getThrowableList(cause).stream().anyMatch(t -> t instanceof SerializationException))
                ctx.close();
        }
    }

}
