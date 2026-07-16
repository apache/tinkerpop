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
package org.apache.tinkerpop.gremlin.jsr223;

import org.junit.Test;

import java.util.Optional;

import static org.apache.tinkerpop.gremlin.jsr223.GremlinScriptChecker.EMPTY_RESULT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class GremlinScriptCheckerTest {

    @Test
    public void shouldNotFindAResult() {
        final GremlinScriptChecker.Result r = GremlinScriptChecker.parse("g.with(true).V().out('knows')");
        assertEquals(Optional.empty(), r.getTimeout());
        assertEquals(Optional.empty(), r.getMaterializeProperties());
    }

    @Test
    public void shouldReturnEmpty() {
        final GremlinScriptChecker.Result r = GremlinScriptChecker.parse("");
        assertSame(EMPTY_RESULT, r);
        assertEquals(Optional.empty(), r.getTimeout());
        assertEquals(Optional.empty(), r.getMaterializeProperties());
    }

    @Test
    public void shouldNotFindTimeoutCozWeCommentedItOut() {
        assertEquals(Optional.empty(), GremlinScriptChecker.parse("g.\n" +
                "                                                  // with('timeoutMillis', 1000L).\n" +
                "                                                  with(true).V().out('knows')").getTimeout());
    }

    @Test
    public void shouldIdentifyTimeoutWithOddSpacing() {
        assertEquals(1000, GremlinScriptChecker.parse("g.with('timeoutMillis' , 1000L).with(true).V().out('knows')").
                getTimeout().get().longValue());
        assertEquals(1000, GremlinScriptChecker.parse("g.with('timeoutMillis'   ,  1000L).with(true).V().out('knows')").
                getTimeout().get().longValue());
        assertEquals(1000, GremlinScriptChecker.parse("g.with('timeoutMillis',1000L).with(true).V().out('knows')").
                getTimeout().get().longValue());
    }

    @Test
    public void shouldIdentifyTimeoutWithLowerL() {
        assertEquals(1000, GremlinScriptChecker.parse("g.with('timeoutMillis', 1000l).with(true).V().out('knows')").
                getTimeout().get().longValue());
        assertEquals(1000, GremlinScriptChecker.parse("g.with('timeoutMillis', 1000L).with(true).V().out('knows')").
                getTimeout().get().longValue());
    }

    @Test
    public void shouldIdentifyTimeoutWithNoL() {
        assertEquals(1000, GremlinScriptChecker.parse("g.with('timeoutMillis', 1000).with(true).V().out('knows')").
                getTimeout().get().longValue());
        assertEquals(1000, GremlinScriptChecker.parse("g.with('timeoutMillis', 1000).with(true).V().out('knows')").
                getTimeout().get().longValue());
    }

    @Test
    public void shouldIdentifyTimeoutAsStringKeySingleQuoted() {
        assertEquals(1000, GremlinScriptChecker.parse("g.with('timeoutMillis', 1000L).with(true).V().out('knows')").
                getTimeout().get().longValue());
        assertEquals(1000, GremlinScriptChecker.parse("g.with('timeoutMillis', 1000L).with(true).V().out('knows')").
                getTimeout().get().longValue());
    }

    @Test
    public void shouldIdentifyTimeoutAsStringKeyDoubleQuoted() {
        assertEquals(1000, GremlinScriptChecker.parse("g.with(\"timeoutMillis\", 1000L).with(true).V().out('knows')").
                getTimeout().get().longValue());
    }

    @Test
    public void shouldIdentifyTimeoutAsTokenKey() {
        assertEquals(1000, GremlinScriptChecker.parse("g.with(Tokens.TIMEOUT_MILLIS, 1000L).with(true).V().out('knows')").
                getTimeout().get().longValue());
        assertEquals(1000, GremlinScriptChecker.parse("g.with(Tokens.TIMEOUT_MILLIS, 1000L).with(true).V().out('knows')").
                getTimeout().get().longValue());
    }

    @Test
    public void shouldIdentifyTimeoutAsTokenKeyWithoutClassName() {
        assertEquals(1000, GremlinScriptChecker.parse("g.with(TIMEOUT_MILLIS, 1000L).with(true).V().out('knows')").
                getTimeout().get().longValue());
        assertEquals(1000, GremlinScriptChecker.parse("g.with(TIMEOUT_MILLIS, 1000L).with(true).V().out('knows')").
                getTimeout().get().longValue());
    }

    @Test
    public void shouldIdentifyMultipleTimeouts() {
        assertEquals(6000, GremlinScriptChecker.parse("g.with('timeoutMillis', 1000L).with(true).V().out('knows');" +
                "g.with('timeoutMillis', 1000L).with(true).V().out('knows');\n" +
                "                                                   //g.with('timeoutMillis', 1000L).with(true).V().out('knows');\n" +
                "                                                   /* g.with('timeoutMillis', 1000L).with(true).V().out('knows');*/\n" +
                "                                                   /* \n" +
                "g.with('timeoutMillis', 1000L).with(true).V().out('knows'); \n" +
                "*/ \n" +
                "                                                   g.with('timeoutMillis', 1000L).with(true).V().out('knows');\n" +
                "                                                   g.with(Tokens.TIMEOUT_MILLIS, 1000L).with(true).V().out('knows');\n" +
                "                                                   g.with(TIMEOUT_MILLIS, 1000L).with(true).V().out('knows');\n" +
                "                                                   g.with('timeoutMillis', 1000L).with(true).V().out('knows');").
                getTimeout().get().longValue());
    }

    @Test
    public void shouldIdentifyMaterializePropertiesSingleQuoted() {
        assertEquals("all", GremlinScriptChecker.parse("g.with('materializeProperties', 'all').with(true).V().out('knows')").
                getMaterializeProperties().get());
        assertEquals("all", GremlinScriptChecker.parse("g.with(\"materializeProperties\", 'all').with(true).V().out('knows')").
                getMaterializeProperties().get());
    }

    @Test
    public void shouldIdentifyMaterializePropertiesDoubleQuoted() {
        assertEquals("all", GremlinScriptChecker.parse("g.with('materializeProperties', \"all\").with(true).V().out('knows')").
                getMaterializeProperties().get());
        assertEquals("all", GremlinScriptChecker.parse("g.with(\"materializeProperties\", \"all\").with(true).V().out('knows')").
                getMaterializeProperties().get());
    }

    @Test
    public void shouldIdentifyMaterializePropertiesWithEmbeddedQuote() {
        assertEquals("te's\"t", GremlinScriptChecker.parse("g.with('materializeProperties', \"te's\"t\").with(true).V().out('knows')").
                getMaterializeProperties().get());
    }

    @Test
    public void shouldIdentifyMaterializePropertiesAsTokenKeyWithoutClassName() {
        assertEquals("all", GremlinScriptChecker.parse("g.with(ARGS_MATERIALIZE_PROPERTIES, 'all').with(true).V().out('knows')").
                getMaterializeProperties().get());
        assertEquals("all", GremlinScriptChecker.parse("g.with(ARGS_MATERIALIZE_PROPERTIES, \"all\").with(true).V().out('knows')").
                getMaterializeProperties().get());
    }

    @Test
    public void shouldIdentifyMaterializePropertiesAsTokenKey() {
        assertEquals("all", GremlinScriptChecker.parse("g.with(Tokens.ARGS_MATERIALIZE_PROPERTIES, 'all').with(true).V().out('knows')").
                getMaterializeProperties().get());
        assertEquals("all", GremlinScriptChecker.parse("g.with(Tokens.ARGS_MATERIALIZE_PROPERTIES, \"all\").with(true).V().out('knows')").
                getMaterializeProperties().get());
    }

    @Test
    public void shouldIdentifyMultipleMaterializeProperties() {
        assertEquals("all", GremlinScriptChecker.parse("g.with('materializeProperties', \"hello\").with(\"materializeProperties\", 'world').with('materializeProperties', 'all').with(true).V().out('knows')").
                getMaterializeProperties().get());
    }

    @Test
    public void shouldFindAllResults() {
        final GremlinScriptChecker.Result r = GremlinScriptChecker.parse(
                "g.with('timeoutMillis', 1000).with(true).with(\"materializeProperties\", 'all').V().out('knows')");
        assertEquals(1000, r.getTimeout().get().longValue());
        assertEquals("all", r.getMaterializeProperties().get());
    }

    @Test
    public void shouldParseLong() {
        assertEquals(1000, GremlinScriptChecker.parse("g.with('timeoutMillis', 1000L).addV().property(id, 'blue').as('b').\n" +
                "  addV().property(id, 'orange').as('o').\n" +
                "  addV().property(id, 'red').as('r').\n" +
                "  addV().property(id, 'green').as('g').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('b').\n" +
                "  addE('bridge').from('g').to('o').\n" +
                "  addE('bridge').from('g').to('r').\n" +
                "  addE('bridge').from('g').to('r').\n" +
                "  addE('bridge').from('o').to('b').\n" +
                "  addE('bridge').from('o').to('b').\n" +
                "  addE('bridge').from('o').to('r').\n" +
                "  addE('bridge').from('o').to('r').iterate()").
                getTimeout().get().longValue());
    }

    @Test
    public void shouldIdentifyLanguageStringKey() {
        assertEquals("gremlin-groovy", GremlinScriptChecker.parse("g.with('language', 'gremlin-groovy').V().out('knows')").
                getLanguage().get());
        assertEquals("gremlin-groovy", GremlinScriptChecker.parse("g.with(\"language\", \"gremlin-groovy\").V().out('knows')").
                getLanguage().get());
    }

    @Test
    public void shouldIdentifyLanguageAsTokenKey() {
        assertEquals("gremlin-groovy", GremlinScriptChecker.parse("g.with(ARGS_LANGUAGE, 'gremlin-groovy').V().out('knows')").
                getLanguage().get());
        assertEquals("gremlin-groovy", GremlinScriptChecker.parse("g.with(Tokens.ARGS_LANGUAGE, 'gremlin-groovy').V().out('knows')").
                getLanguage().get());
    }

    @Test
    public void shouldIdentifyMultipleLanguagesUsingLast() {
        assertEquals("gremlin-lang", GremlinScriptChecker.parse("g.with('language', 'gremlin-groovy').with('language', 'gremlin-lang').V()").
                getLanguage().get());
    }

    @Test
    public void shouldIdentifyBatchSizeStringKey() {
        assertEquals("10", GremlinScriptChecker.parse("g.with('batchSize', 10).V().out('knows')").
                getBatchSize().get());
        assertEquals("10", GremlinScriptChecker.parse("g.with(\"batchSize\", 10).V().out('knows')").
                getBatchSize().get());
    }

    @Test
    public void shouldIdentifyBatchSizeAsTokenKey() {
        assertEquals("25", GremlinScriptChecker.parse("g.with(ARGS_BATCH_SIZE, 25).V().out('knows')").
                getBatchSize().get());
        assertEquals("25", GremlinScriptChecker.parse("g.with(Tokens.ARGS_BATCH_SIZE, 25).V().out('knows')").
                getBatchSize().get());
    }

    @Test
    public void shouldIdentifyMultipleBatchSizesUsingLast() {
        assertEquals("64", GremlinScriptChecker.parse("g.with('batchSize', 10).with('batchSize', 64).V()").
                getBatchSize().get());
    }

    @Test
    public void shouldCaptureBatchSizeAboveIntegerMaxUnparsed() {
        // the checker captures the raw string without parsing, so a value above Integer.MAX_VALUE is returned as-is
        // (rather than throwing) and the server can reject it as a bad request.
        assertEquals("2147483648", GremlinScriptChecker.parse("g.with('batchSize', 2147483648).V()").
                getBatchSize().get());
    }

    @Test
    public void shouldIdentifyBulkResultsStringKey() {
        assertEquals(Boolean.TRUE, GremlinScriptChecker.parse("g.with('bulkResults', true).V().out('knows')").
                getBulkResults().get());
        assertEquals(Boolean.FALSE, GremlinScriptChecker.parse("g.with(\"bulkResults\", false).V().out('knows')").
                getBulkResults().get());
    }

    @Test
    public void shouldIdentifyBulkResultsAsTokenKey() {
        assertEquals(Boolean.TRUE, GremlinScriptChecker.parse("g.with(BULK_RESULTS, true).V().out('knows')").
                getBulkResults().get());
        assertEquals(Boolean.TRUE, GremlinScriptChecker.parse("g.with(Tokens.BULK_RESULTS, true).V().out('knows')").
                getBulkResults().get());
    }

    @Test
    public void shouldIdentifyMultipleBulkResultsUsingLast() {
        assertEquals(Boolean.FALSE, GremlinScriptChecker.parse("g.with('bulkResults', true).with('bulkResults', false).V()").
                getBulkResults().get());
    }

    @Test
    public void shouldNotIdentifyBindingsFromScript() {
        // parameters is intentionally NOT scraped from a script (field/header-only)
        final GremlinScriptChecker.Result r = GremlinScriptChecker.parse("g.with('parameters', '[\"x\":1]').V(x)");
        assertEquals(Optional.empty(), r.getTimeout());
        assertEquals(Optional.empty(), r.getLanguage());
        assertEquals(Optional.empty(), r.getBatchSize());
        assertEquals(Optional.empty(), r.getBulkResults());
    }

    @Test
    public void shouldNotIdentifyTransactionIdFromScript() {
        // transactionId is intentionally NOT scraped from a script (field/header-only)
        final GremlinScriptChecker.Result r = GremlinScriptChecker.parse("g.with('transactionId', 'abc').V()");
        assertEquals(Optional.empty(), r.getTimeout());
        assertEquals(Optional.empty(), r.getLanguage());
        assertEquals(Optional.empty(), r.getBatchSize());
        assertEquals(Optional.empty(), r.getBulkResults());
    }

    @Test
    public void shouldNotIdentifyGFromScript() {
        // g is intentionally NOT scraped from a script due to the complex interactions between different script
        // engines and transactions - it must be supplied as a request field.
        final GremlinScriptChecker.Result r = GremlinScriptChecker.parse("g.with('g', 'myGraph').V().out('knows')");
        assertEquals(Optional.empty(), r.getTimeout());
        assertEquals(Optional.empty(), r.getLanguage());
        assertEquals(Optional.empty(), r.getBatchSize());
        assertEquals(Optional.empty(), r.getBulkResults());
    }

    @Test
    public void shouldFindAllFiveResults() {
        final GremlinScriptChecker.Result r = GremlinScriptChecker.parse(
                "g.with('timeoutMillis', 1000).with('materializeProperties', 'all').with('language', 'gremlin-groovy')." +
                "with('batchSize', 32).with('bulkResults', true).V().out('knows')");
        assertEquals(1000, r.getTimeout().get().longValue());
        assertEquals("all", r.getMaterializeProperties().get());
        assertEquals("gremlin-groovy", r.getLanguage().get());
        assertEquals("32", r.getBatchSize().get());
        assertEquals(Boolean.TRUE, r.getBulkResults().get());
    }
}
