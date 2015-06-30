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

import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

/**
 * Holder for internal handler classes used in constructing the channel pipeline.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
final class Handler {

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
        protected void channelRead0(final ChannelHandlerContext channelHandlerContext, final ResponseMessage response) throws Exception {
            try {
                final ResponseStatusCode statusCode = response.getStatus().getCode();
                if (statusCode == ResponseStatusCode.SUCCESS || statusCode == ResponseStatusCode.PARTIAL_CONTENT) {
                    final Object data = response.getResult().getData();
                    if (data instanceof List) {
                        // unrolls the collection into individual results to be handled by the queue.
                        final List<Object> listToUnroll = (List<Object>) data;
                        final ResultQueue queue = pending.get(response.getRequestId());
                        listToUnroll.forEach(item -> queue.add(new Result(item)));
                    } else {
                        // since this is not a list it can just be added to the queue
                        pending.get(response.getRequestId()).add(new Result(response.getResult().getData()));
                    }
                } else {
                    // this is a "success" but represents no results otherwise it is an error
                    if (statusCode != ResponseStatusCode.NO_CONTENT)
                        pending.get(response.getRequestId()).markError(new ResponseException(response.getStatus().getCode(), response.getStatus().getMessage()));
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

        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
            // if this happens enough times (like the client is unable to deserialize a response) the pending
            // messages queue will not clear.  wonder if there is some way to cope with that.  of course, if
            // there are that many failures someone would take notice and hopefully stop the client.
            logger.error("Could not process the response - correct the problem and restart the driver.", cause);

            // the channel is getting closed because of something pretty bad so release all the completeable
            // futures out there
            pending.entrySet().stream().forEach(kv -> kv.getValue().markError(cause));

            ctx.close();
        }
    }

}
