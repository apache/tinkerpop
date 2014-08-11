package com.tinkerpop.gremlin.driver;

import com.tinkerpop.gremlin.driver.exception.ResponseException;
import com.tinkerpop.gremlin.driver.message.ResponseMessage;
import com.tinkerpop.gremlin.driver.message.ResultCode;
import com.tinkerpop.gremlin.driver.message.ResultType;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        private static final Logger logger = LoggerFactory.getLogger(GremlinResponseHandler.class);

        private final ConcurrentMap<UUID, ResponseQueue> pending;

        public GremlinResponseHandler(final ConcurrentMap<UUID, ResponseQueue> pending) {
            this.pending = pending;
        }

        @Override
        protected void channelRead0(final ChannelHandlerContext channelHandlerContext, final ResponseMessage response) throws Exception {
            try {
                if (response.getCode() == ResultCode.SUCCESS) {
                    if (response.getResultType() == ResultType.OBJECT)
                        pending.get(response.getRequestId()).add(response);
                    else if (response.getResultType() == ResultType.COLLECTION) {
                        // unrolls the collection into individual response messages to be handled by the queue
                        final List<Object> listToUnroll = (List<Object>) response.getResult();
                        final ResponseQueue queue = pending.get(response.getRequestId());
                        listToUnroll.forEach(item -> queue.add(
                                ResponseMessage.build(response.getRequestId())
                                        .result(item).create()));
                    } else if (response.getResultType() == ResultType.EMPTY) {
                        // there is nothing to do with ResultType.EMPTY - it will simply be marked complete with
                        // a success terminator
                    } else {
                        logger.warn("Received an invalid ResultType of [{}] - marking request {} as being in error. Please report as this issue.", response.getResultType(), response.getRequestId());
                        pending.get(response.getRequestId()).markError(new RuntimeException(response.getResult().toString()));
                    }
                } else if (response.getCode() == ResultCode.SUCCESS_TERMINATOR)
                    pending.remove(response.getRequestId()).markComplete();
                else
                    pending.get(response.getRequestId()).markError(new ResponseException(response.getCode(), response.getResult().toString()));
            } finally {
                ReferenceCountUtil.release(response);
            }
        }
    }

}
