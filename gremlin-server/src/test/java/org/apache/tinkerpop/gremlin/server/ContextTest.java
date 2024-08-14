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
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;

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

    @Test
    public void shouldParseParametersFromScriptRequest()
    {
        final ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
        final RequestMessage request =
                RequestMessage.build("g.with('evaluationTimeout', 1000).with(true).with('materializeProperties', 'tokens').V().out('knows')")
                    .create();
        final Settings settings = new Settings();
        final Context context = new Context(request, ctx, settings, null, null, null);

        assertEquals(1000, context.getRequestTimeout());
        assertEquals("tokens", context.getMaterializeProperties());
    }

    @Test
    public void shouldSkipInvalidMaterializePropertiesParameterFromScriptRequest()
    {
        final ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
        final RequestMessage request =
                RequestMessage.build("g.with('evaluationTimeout', 1000).with(true).with('materializeProperties', 'some-invalid-value').V().out('knows')")
                    .create();
        final Settings settings = new Settings();
        final Context context = new Context(request, ctx, settings, null, null, null);

        assertEquals(1000, context.getRequestTimeout());
        // "all" is default value
        assertEquals("all", context.getMaterializeProperties());
    }
}