package com.tinkerpop.gremlin.driver;

import com.tinkerpop.gremlin.driver.exception.ResponseException;
import com.tinkerpop.gremlin.driver.message.ResponseMessage;
import com.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.ReferenceCountUtil;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

/**
 * Traverser for internal handler classes for constructing the Channel Pipeline.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class Handler {

    static class GremlinResponseHandler extends SimpleChannelInboundHandler<ResponseMessage> {
        private final ConcurrentMap<UUID, ResponseQueue> pending;

        public GremlinResponseHandler(final ConcurrentMap<UUID, ResponseQueue> pending) {
            this.pending = pending;
        }

        @Override
        protected void channelRead0(final ChannelHandlerContext channelHandlerContext, final ResponseMessage response) throws Exception {
            try {
                if (response.getStatus().getCode() == ResponseStatusCode.SUCCESS) {
                    final Object data = response.getResult().getData();
                    if (data instanceof List) {
                        // unrolls the collection into individual response messages to be handled by the queue
                        final List<Object> listToUnroll = (List<Object>) data;
                        final ResponseQueue queue = pending.get(response.getRequestId());
                        listToUnroll.forEach(item -> queue.add(
                                ResponseMessage.build(response.getRequestId())
                                        .result(item).create()));
                    } else {
                        // since this is not a list it can just be added to the queue
                        pending.get(response.getRequestId()).add(response);
                    }
                } else if (response.getStatus().getCode() == ResponseStatusCode.SUCCESS_TERMINATOR)
                    pending.remove(response.getRequestId()).markComplete();
                else
                    pending.get(response.getRequestId()).markError(new ResponseException(response.getStatus().getCode(), response.getStatus().getMessage()));
            } finally {
                ReferenceCountUtil.release(response);
            }
        }
    }

}
