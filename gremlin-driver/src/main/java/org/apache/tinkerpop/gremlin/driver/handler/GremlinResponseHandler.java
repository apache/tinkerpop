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
package org.apache.tinkerpop.gremlin.driver.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.AttributeMap;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultQueue;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import static org.apache.tinkerpop.gremlin.driver.handler.HttpGremlinRequestEncoder.REQUEST_ID;

/**
 * Takes a map of requests pending responses and writes responses to the {@link ResultQueue} of a request
 * as the {@link ResponseMessage} objects are deserialized.
 */
public class GremlinResponseHandler extends SimpleChannelInboundHandler<ResponseMessage> {
    private static final Logger logger = LoggerFactory.getLogger(GremlinResponseHandler.class);
    private final ConcurrentMap<UUID, ResultQueue> pending;

    public GremlinResponseHandler(final ConcurrentMap<UUID, ResultQueue> pending) {
        this.pending = pending;
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
        // occurs when the server shuts down in a disorderly fashion, otherwise in an orderly shutdown the server
        // should fire off a close message which will properly release the driver.
        super.channelInactive(ctx);

        // the channel isn't going to get anymore results as it is closed so release all pending requests
        pending.values().forEach(val -> val.markError(new IllegalStateException("Connection to server is no longer active")));
        pending.clear();
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext channelHandlerContext, final ResponseMessage response) throws Exception {
        final UUID requestId = ((AttributeMap) channelHandlerContext).attr(REQUEST_ID).get();

        final ResponseStatusCode statusCode = response.getStatus() == null ? ResponseStatusCode.PARTIAL_CONTENT : response.getStatus().getCode();
        final ResultQueue queue = pending.get(requestId);
        System.out.println("Handler.GremlinResponseHandler get requestId: " + requestId);
        if (response.getResult().getData() != null) {
            System.out.println("Handler.GremlinResponseHandler payload size: " + ((List) response.getResult().getData()).size());
        }

        if (statusCode == ResponseStatusCode.SUCCESS || statusCode == ResponseStatusCode.PARTIAL_CONTENT) {
            final Object data = response.getResult().getData();

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
            // this is a "success" but represents no results otherwise it is an error
            if (statusCode != ResponseStatusCode.NO_CONTENT) {
                final Map<String, Object> attributes = response.getStatus().getAttributes();
                final String stackTrace = attributes.containsKey(Tokens.STATUS_ATTRIBUTE_STACK_TRACE) ?
                        (String) attributes.get(Tokens.STATUS_ATTRIBUTE_STACK_TRACE) : null;
                final List<String> exceptions = attributes.containsKey(Tokens.STATUS_ATTRIBUTE_EXCEPTIONS) ?
                        (List<String>) attributes.get(Tokens.STATUS_ATTRIBUTE_EXCEPTIONS) : null;
                queue.markError(new ResponseException(response.getStatus().getCode(), response.getStatus().getMessage(),
                        exceptions, stackTrace, cleanStatusAttributes(attributes)));
            }
        }

        // todo:
        // as this is a non-PARTIAL_CONTENT code - the stream is done.
        if (statusCode != ResponseStatusCode.PARTIAL_CONTENT) {
            pending.remove(requestId).markComplete(response.getStatus().getAttributes());
        }

        System.out.println("----------------------------");
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

    // todo: solution is not decided
    private Map<String, Object> cleanStatusAttributes(final Map<String, Object> statusAttributes) {
        final Map<String, Object> m = new HashMap<>();
        statusAttributes.forEach((k, v) -> {
            if (!k.equals(Tokens.STATUS_ATTRIBUTE_EXCEPTIONS) && !k.equals(Tokens.STATUS_ATTRIBUTE_STACK_TRACE))
                m.put(k, v);
        });
        return m;
    }
}
