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

import com.codahale.metrics.Meter;
import io.netty.util.CharsetUtil;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.driver.ser.MessageTextSerializer;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.util.ExceptionHelper;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @deprecated As of release 3.1.1-incubating, replaced by {@link NioGremlinResponseFrameEncoder} and
 * {@link GremlinResponseFrameEncoder}
 * @see <a href="https://issues.apache.org/jira/browse/TINKERPOP-1035">TINKERPOP-1035</a>
 */
@Deprecated
@ChannelHandler.Sharable
public class NioGremlinResponseEncoder extends MessageToByteEncoder<ResponseMessage> {
    private static final Logger logger = LoggerFactory.getLogger(NioGremlinResponseEncoder.class);
    static final Meter errorMeter = MetricManager.INSTANCE.getMeter(name(GremlinServer.class, "errors"));

    @Override
    protected void encode(final ChannelHandlerContext ctx, final ResponseMessage responseMessage, final ByteBuf byteBuf) throws Exception {
        final MessageSerializer serializer = ctx.channel().attr(StateKey.SERIALIZER).get();
        final boolean useBinary = ctx.channel().attr(StateKey.USE_BINARY).get();

        try {
            if (!responseMessage.getStatus().getCode().isSuccess())
                errorMeter.mark();

            if (useBinary) {
                final ByteBuf bytes = serializer.serializeResponseAsBinary(responseMessage, ctx.alloc());
                byteBuf.writeInt(bytes.capacity());
                byteBuf.writeBytes(bytes);
                bytes.release();
            } else {
                // the expectation is that the GremlinTextRequestDecoder will have placed a MessageTextSerializer
                // instance on the channel.
                final MessageTextSerializer textSerializer = (MessageTextSerializer) serializer;
                final byte [] bytes = textSerializer.serializeResponseAsString(responseMessage).getBytes(CharsetUtil.UTF_8);
                byteBuf.writeInt(bytes.length);
                byteBuf.writeBytes(bytes);
            }
        } catch (Exception ex) {
            errorMeter.mark();
            logger.warn("The result [{}] in the request {} could not be serialized and returned.", responseMessage.getResult(), responseMessage.getRequestId(), ex);
            final String errorMessage = String.format("Error during serialization: %s", ExceptionHelper.getMessageFromExceptionOrCause(ex));
            final ResponseMessage error = ResponseMessage.build(responseMessage.getRequestId())
                    .statusMessage(errorMessage)
                    .statusAttributeException(ex)
                    .code(ResponseStatusCode.SERVER_ERROR_SERIALIZATION).create();
            if (useBinary) {
                final ByteBuf bytes = serializer.serializeResponseAsBinary(error, ctx.alloc());
                byteBuf.writeInt(bytes.capacity());
                byteBuf.writeBytes(bytes);
                bytes.release();
            } else {
                final MessageTextSerializer textSerializer = (MessageTextSerializer) serializer;
                final byte [] bytes = textSerializer.serializeResponseAsString(error).getBytes(CharsetUtil.UTF_8);
                byteBuf.writeInt(bytes.length);
                byteBuf.writeBytes(bytes);
            }
        }
    }
}
