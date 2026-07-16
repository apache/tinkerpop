/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

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
                RequestMessage.build("g.with('timeoutMillis', 1000).with(true).with('materializeProperties', 'tokens').V().out('knows')")
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
                RequestMessage.build("g.with('timeoutMillis', 1000).with(true).with('materializeProperties', 'some-invalid-value').V().out('knows')")
                    .create();
        final Settings settings = new Settings();
        final Context context = new Context(request, ctx, settings, null, null, null);

        assertEquals(1000, context.getRequestTimeout());
        // "all" is default value
        assertEquals("all", context.getMaterializeProperties());
    }

    private static Context newContext(final RequestMessage request) {
        final ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
        return new Context(request, ctx, new Settings(), null, null, null);
    }

    @Test
    public void shouldResolveTimeoutFromScriptOverField() {
        final RequestMessage request = RequestMessage.build("g.with('timeoutMillis', 1000).V()")
                .addTimeoutMillis(5000).create();
        assertEquals(1000, newContext(request).getRequestTimeout());
    }

    @Test
    public void shouldResolveTimeoutFromFieldWhenNoScript() {
        final RequestMessage request = RequestMessage.build("g.V()").addTimeoutMillis(5000).create();
        assertEquals(5000, newContext(request).getRequestTimeout());
    }

    @Test
    public void shouldResolveTimeoutFromDefaultWhenNeither() {
        final RequestMessage request = RequestMessage.build("g.V()").create();
        assertEquals(new Settings().getTimeoutMillis(), newContext(request).getRequestTimeout());
    }

    @Test
    public void shouldResolveMaterializePropertiesFromScriptOverField() {
        final RequestMessage request = RequestMessage.build("g.with('materializeProperties', 'tokens').V()")
                .addMaterializeProperties("all").create();
        assertEquals("tokens", newContext(request).getMaterializeProperties());
    }

    @Test
    public void shouldResolveMaterializePropertiesFromFieldWhenNoScript() {
        final RequestMessage request = RequestMessage.build("g.V()").addMaterializeProperties("tokens").create();
        assertEquals("tokens", newContext(request).getMaterializeProperties());
    }

    @Test
    public void shouldResolveMaterializePropertiesFromDefaultWhenNeither() {
        final RequestMessage request = RequestMessage.build("g.V()").create();
        assertEquals("all", newContext(request).getMaterializeProperties());
    }

    @Test
    public void shouldResolveLanguageFromScriptOverField() {
        final RequestMessage request = RequestMessage.build("g.with('language', 'gremlin-groovy').V()")
                .addLanguage("gremlin-lang").create();
        assertEquals("gremlin-groovy", newContext(request).getLanguage());
    }

    @Test
    public void shouldResolveLanguageFromFieldWhenNoScript() {
        final RequestMessage request = RequestMessage.build("g.V()").addLanguage("gremlin-groovy").create();
        assertEquals("gremlin-groovy", newContext(request).getLanguage());
    }

    @Test
    public void shouldResolveLanguageFromDefaultWhenNeither() {
        final RequestMessage request = RequestMessage.build("g.V()").create();
        assertEquals("gremlin-lang", newContext(request).getLanguage());
    }

    @Test
    public void shouldResolveBatchSizeFromScriptOverField() throws Exception {
        final RequestMessage request = RequestMessage.build("g.with('batchSize', 10).V()")
                .addChunkSize(500).create();
        assertEquals(10, newContext(request).getBatchSize());
    }

    @Test
    public void shouldResolveBatchSizeFromFieldWhenNoScript() throws Exception {
        final RequestMessage request = RequestMessage.build("g.V()").addChunkSize(500).create();
        assertEquals(500, newContext(request).getBatchSize());
    }

    @Test
    public void shouldResolveBatchSizeFromDefaultWhenNeither() throws Exception {
        final RequestMessage request = RequestMessage.build("g.V()").create();
        assertEquals(new Settings().resultIterationBatchSize, newContext(request).getBatchSize());
    }

    @Test
    public void shouldThrowOnNonPositiveBatchSize() {
        // getBatchSize() parses and validates, throwing a bad-request ProcessingException for a non-positive value
        // (from either the script or the request field) rather than returning a value that would stall iteration.
        final RequestMessage fromScript = RequestMessage.build("g.with('batchSize', 0).V()").create();
        assertThrows(ProcessingException.class, () -> newContext(fromScript).getBatchSize());

        final RequestMessage fromField = RequestMessage.build("g.V()").addChunkSize(0).create();
        assertThrows(ProcessingException.class, () -> newContext(fromField).getBatchSize());
    }

    @Test
    public void shouldThrowOnBatchSizeAboveIntegerMax() {
        // a script-embedded value above Integer.MAX_VALUE cannot be applied and is rejected as a bad request rather
        // than throwing an uncaught NumberFormatException while constructing Context.
        final RequestMessage request = RequestMessage.build("g.with('batchSize', 2147483648).V()").create();
        assertThrows(ProcessingException.class, () -> newContext(request).getBatchSize());
    }

    @Test
    public void shouldResolveBulkResultsFromScriptOverField() {
        final RequestMessage request = RequestMessage.build("g.with('bulkResults', false).V()")
                .addBulkResults(true).create();
        assertFalse(newContext(request).getBulkResults());
    }

    @Test
    public void shouldResolveBulkResultsFromFieldWhenNoScript() {
        final RequestMessage request = RequestMessage.build("g.V()").addBulkResults(true).create();
        assertTrue(newContext(request).getBulkResults());
    }

    @Test
    public void shouldResolveBulkResultsFromDefaultWhenNeither() {
        final RequestMessage request = RequestMessage.build("g.V()").create();
        assertFalse(newContext(request).getBulkResults());
    }

    @Test
    public void shouldNotHonorParametersFromScript() {
        // an embedded with('parameters', ...) must not become the request's parameters; only the field is authoritative.
        final RequestMessage request = RequestMessage.build("g.with('parameters', '[\"x\":1]').V(x)")
                .addParameters("[\"y\":2]").create();
        final Context context = newContext(request);
        // Context does not expose parsed parameters from the script; the field value remains the source of truth.
        assertEquals("[\"y\":2]", context.getRequestMessage().getField(Tokens.ARGS_PARAMETERS));
    }

    @Test
    public void shouldNotHonorTransactionIdFromScript() {
        final RequestMessage request = RequestMessage.build("g.with('transactionId', 'fromScript').V()")
                .addTransactionId("fromField").create();
        assertEquals("fromField", newContext(request).getTransactionId());
    }
}
