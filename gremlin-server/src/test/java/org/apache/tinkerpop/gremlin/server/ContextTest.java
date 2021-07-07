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
package org.apache.tinkerpop.gremlin.server;

import io.netty.channel.ChannelHandlerContext;
import nl.altindag.log.LogCaptor;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.server.handler.Frame;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.UUID;
import java.util.function.BiFunction;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

@RunWith(Parameterized.class)
public class ContextTest {

    private static LogCaptor logCaptor;

    @BeforeClass
    public static void setupLogCaptor() {
        logCaptor = LogCaptor.forRoot();
    }

    @AfterClass
    public static void tearDownAfterClass() {
        logCaptor.close();
    }

    @Before
    public void setupForEachTest() {
        logCaptor.clearLogs();
    }

    @Parameterized.Parameter(value = 0)
    public BiFunction<Context, ResponseStatusCode, Void> writeInvoker;

    private final ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    private final RequestMessage request = RequestMessage.build("test").create();
    private final Settings settings = new Settings();
    private final Context context = new Context(request, ctx, settings, null, null, null);

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {
                    new BiFunction<Context, ResponseStatusCode, Void>() {
                        @Override
                        public Void apply(final Context context, final ResponseStatusCode code) {
                            context.writeAndFlush(code, "testMessage");
                            return null;
                        }

                        @Override
                        public String toString() {
                            return "writeAndFlush(ResponseStatusCode, Object)";
                        }
                    }
                }, {
                    new BiFunction<Context, ResponseStatusCode, Void>() {
                        @Override
                        public Void apply(final Context context, final ResponseStatusCode code) {
                            context.writeAndFlush(ResponseMessage.build(UUID.randomUUID()).code(code).create());
                            return null;
                        }

                        @Override
                        public String toString() {
                            return "writeAndFlush(ResponseMessage)";
                        }
                    }
                },
        });
    }

    @Test
    public void shouldAllowMultipleNonFinalResponses() {
        writeInvoker.apply(context, ResponseStatusCode.AUTHENTICATE);
        Mockito.verify(ctx, Mockito.times(1)).write(Mockito.any());
        Mockito.verify(ctx, Mockito.times(1)).flush();

        writeInvoker.apply(context, ResponseStatusCode.PARTIAL_CONTENT);
        Mockito.verify(ctx, Mockito.times(2)).write(Mockito.any());
        Mockito.verify(ctx, Mockito.times(2)).flush();

        writeInvoker.apply(context, ResponseStatusCode.PARTIAL_CONTENT);
        Mockito.verify(ctx, Mockito.times(3)).write(Mockito.any());
        Mockito.verify(ctx, Mockito.times(3)).flush();
    }

    @Test
    public void shouldAllowAtMostOneFinalResponse() {
        writeInvoker.apply(context, ResponseStatusCode.AUTHENTICATE);
        Mockito.verify(ctx, Mockito.times(1)).write(Mockito.any());
        Mockito.verify(ctx, Mockito.times(1)).flush();

        writeInvoker.apply(context, ResponseStatusCode.SUCCESS);
        Mockito.verify(ctx, Mockito.times(2)).write(Mockito.any());
        Mockito.verify(ctx, Mockito.times(2)).flush();

        writeInvoker.apply(context, ResponseStatusCode.SERVER_ERROR_TIMEOUT);
        assertThat(logCaptor.getLogs().stream().anyMatch(m ->
                m.matches(".*" + request.getRequestId() + ".*")), is(true));
        assertThat(logCaptor.getLogs().stream().anyMatch(m ->
                m.matches(".*" + ResponseStatusCode.SERVER_ERROR_TIMEOUT + "$")), is(true));

        // ensure there were no other writes to the channel
        Mockito.verify(ctx, Mockito.times(2)).write(Mockito.any());
        Mockito.verify(ctx, Mockito.times(2)).flush();
    }

    @Test
    public void shouldNotAllowNonFinalMessagesAfterFinalResponse() {
        writeInvoker.apply(context, ResponseStatusCode.SERVER_ERROR_TIMEOUT);
        Mockito.verify(ctx, Mockito.times(1)).write(Mockito.any());
        Mockito.verify(ctx, Mockito.times(1)).flush();

        writeInvoker.apply(context, ResponseStatusCode.PARTIAL_CONTENT);
        assertThat(logCaptor.getLogs().stream().anyMatch(m ->
                m.matches(".*" + request.getRequestId() + ".*")), is(true));
        assertThat(logCaptor.getLogs().stream().anyMatch(m ->
                m.matches(".*" + ResponseStatusCode.PARTIAL_CONTENT + "$")), is(true));

        // ensure there were no other writes to the channel
        Mockito.verify(ctx, Mockito.times(1)).write(Mockito.any());
        Mockito.verify(ctx, Mockito.times(1)).flush();
    }

    @Test
    public void shouldReleaseIgnoredFrames() {
        writeInvoker.apply(context, ResponseStatusCode.SERVER_ERROR_TIMEOUT);
        Mockito.verify(ctx, Mockito.times(1)).write(Mockito.any());
        Mockito.verify(ctx, Mockito.times(1)).flush();

        Frame frame = Mockito.mock(Frame.class);
        context.writeAndFlush(ResponseStatusCode.SUCCESS, frame);

        assertThat(logCaptor.getLogs().stream().anyMatch(m ->
                m.matches(".*" + request.getRequestId() + ".*")), is(true));
        assertThat(logCaptor.getLogs().stream().anyMatch(m ->
                m.matches(".*" + ResponseStatusCode.SUCCESS + "$")), is(true));

        // ensure there were no other writes to the channel
        Mockito.verify(ctx, Mockito.times(1)).write(Mockito.any());
        Mockito.verify(ctx, Mockito.times(1)).flush();

        // ensure the frame was released
        Mockito.verify(frame, Mockito.times(1)).tryRelease();
        Mockito.verify(ctx, Mockito.times(1)).flush();
    }
}