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
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.driver.ser.MessageTextSerializer;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.op.session.Session;
import org.apache.tinkerpop.gremlin.server.util.ExceptionHelper;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Ensures that any {@link ResponseMessage} manages to get converted to a {@link Frame}. By converting to {@link Frame}
 * downstream protocols can treat the generic {@link Frame} any way it wants (e.g. write it back as a byte array,
 * websocket frame, etc).
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@ChannelHandler.Sharable
public class GremlinResponseFrameEncoder extends MessageToMessageEncoder<ResponseMessage> {
    private static final Logger logger = LoggerFactory.getLogger(GremlinResponseFrameEncoder.class);
    static final Meter errorMeter = MetricManager.INSTANCE.getMeter(name(GremlinServer.class, "errors"));

    @Override
    protected void encode(final ChannelHandlerContext ctx, final ResponseMessage o, final List<Object> objects) throws Exception {
        final MessageSerializer<?> serializer = ctx.channel().attr(StateKey.SERIALIZER).get();
        final boolean useBinary = ctx.channel().attr(StateKey.USE_BINARY).get();
        final Session session = ctx.channel().attr(StateKey.SESSION).get();

        try {
            if (!o.getStatus().getCode().isSuccess())
                errorMeter.mark();

            if (useBinary) {
                final Frame serialized;

                // if the request came in on a session then the serialization must occur in that same thread, except
                // in the case of an error where we can free the session executor from having to do that job. the
                // problem here is that if the session executor is used in the case of an error and the executor is
                // blocked by parallel requests then there is no thread available to serialize the result and send
                // back the response as the workers get all tied up behind the session executor.
                if (null == session || !o.getStatus().getCode().isSuccess())
                    serialized = new Frame(serializer.serializeResponseAsBinary(o, ctx.alloc()));
                else
                    serialized = new Frame(session.getExecutor().submit(() -> serializer.serializeResponseAsBinary(o, ctx.alloc())).get());

                objects.add(serialized);
            } else {
                // the expectation is that the GremlinTextRequestDecoder will have placed a MessageTextSerializer
                // instance on the channel.
                final MessageTextSerializer<?> textSerializer = (MessageTextSerializer<?>) serializer;

                final Frame serialized;

                // if the request came in on a session then the serialization must occur that same thread except
                // in the case of errors for reasons described above.
                if (null == session || !o.getStatus().getCode().isSuccess())
                    serialized = new Frame(textSerializer.serializeResponseAsString(o));
                else
                    serialized = new Frame(session.getExecutor().submit(() -> textSerializer.serializeResponseAsString(o)).get());

                objects.add(serialized);
            }
        } catch (Exception ex) {
            errorMeter.mark();
            logger.warn("The result [{}] in the request {} could not be serialized and returned.", o.getResult(), o.getRequestId(), ex);
            final String errorMessage = String.format("Error during serialization: %s", ExceptionHelper.getMessageFromExceptionOrCause(ex));
            final ResponseMessage error = ResponseMessage.build(o.getRequestId())
                    .statusMessage(errorMessage)
                    .statusAttributeException(ex)
                    .code(ResponseStatusCode.SERVER_ERROR_SERIALIZATION).create();
            if (useBinary) {
                objects.add(serializer.serializeResponseAsBinary(error, ctx.alloc()));
            } else {
                final MessageTextSerializer<?> textSerializer = (MessageTextSerializer<?>) serializer;
                objects.add(textSerializer.serializeResponseAsString(error));
            }
        }
    }
}
