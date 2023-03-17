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
        assertEquals(Optional.empty(), r.getRequestId());
        assertEquals(Optional.empty(), r.getMaterializeProperties());
    }

    @Test
    public void shouldReturnEmpty() {
        final GremlinScriptChecker.Result r = GremlinScriptChecker.parse("");
        assertSame(EMPTY_RESULT, r);
        assertEquals(Optional.empty(), r.getTimeout());
        assertEquals(Optional.empty(), r.getRequestId());
        assertEquals(Optional.empty(), r.getMaterializeProperties());
    }

    @Test
    public void shouldNotFindTimeoutCozWeCommentedItOut() {
        assertEquals(Optional.empty(), GremlinScriptChecker.parse("g.\n" +
                "                                                  // with('evaluationTimeout', 1000L).\n" +
                "                                                  with(true).V().out('knows')").getTimeout());
    }

    @Test
    public void shouldIdentifyTimeoutWithOddSpacing() {
        assertEquals(1000, GremlinScriptChecker.parse("g.with('evaluationTimeout' , 1000L).with(true).V().out('knows')").
                getTimeout().get().longValue());
        assertEquals(1000, GremlinScriptChecker.parse("g.with('scriptEvaluationTimeout'   ,  1000L).with(true).V().out('knows')").
                getTimeout().get().longValue());
        assertEquals(1000, GremlinScriptChecker.parse("g.with('evaluationTimeout',1000L).with(true).V().out('knows')").
                getTimeout().get().longValue());
    }

    @Test
    public void shouldIdentifyRequestIdWithOddSpacing() {
        assertEquals("4F53FB59-CFC9-4984-B477-452073A352FD", GremlinScriptChecker.parse("g.with('requestId' , '4F53FB59-CFC9-4984-B477-452073A352FD').with(true).V().out('knows')").
                getRequestId().get());
        assertEquals("4F53FB59-CFC9-4984-B477-452073A352FD", GremlinScriptChecker.parse("g.with('requestId'   ,  '4F53FB59-CFC9-4984-B477-452073A352FD').with(true).V().out('knows')").
                getRequestId().get());
        assertEquals("4F53FB59-CFC9-4984-B477-452073A352FD", GremlinScriptChecker.parse("g.with('requestId','4F53FB59-CFC9-4984-B477-452073A352FD').with(true).V().out('knows')").
                getRequestId().get());
    }

    @Test
    public void shouldIdentifyRequestIdWithEmbeddedQuote() {
        assertEquals("te\"st", GremlinScriptChecker.parse("g.with('requestId','te\"st').with(true).V().out('knows')").
                getRequestId().get());
        assertEquals("te\\\"st", GremlinScriptChecker.parse("g.with('requestId', \"te\\\"st\").with(true).V().out('knows')").
                getRequestId().get());
    }

    @Test
    public void shouldIdentifyTimeoutWithLowerL() {
        assertEquals(1000, GremlinScriptChecker.parse("g.with('evaluationTimeout', 1000l).with(true).V().out('knows')").
                getTimeout().get().longValue());
        assertEquals(1000, GremlinScriptChecker.parse("g.with('scriptEvaluationTimeout', 1000L).with(true).V().out('knows')").
                getTimeout().get().longValue());
    }

    @Test
    public void shouldIdentifyTimeoutWithNoL() {
        assertEquals(1000, GremlinScriptChecker.parse("g.with('evaluationTimeout', 1000).with(true).V().out('knows')").
                getTimeout().get().longValue());
        assertEquals(1000, GremlinScriptChecker.parse("g.with('scriptEvaluationTimeout', 1000).with(true).V().out('knows')").
                getTimeout().get().longValue());
    }

    @Test
    public void shouldIdentifyTimeoutAsStringKeySingleQuoted() {
        assertEquals(1000, GremlinScriptChecker.parse("g.with('evaluationTimeout', 1000L).with(true).V().out('knows')").
                getTimeout().get().longValue());
        assertEquals(1000, GremlinScriptChecker.parse("g.with('scriptEvaluationTimeout', 1000L).with(true).V().out('knows')").
                getTimeout().get().longValue());
    }

    @Test
    public void shouldIdentifyRequestIdAsStringKeySingleQuoted() {
        assertEquals("db024fca-ed15-4375-95de-4c6106aef895", GremlinScriptChecker.parse("g.with('requestId', 'db024fca-ed15-4375-95de-4c6106aef895').with(true).V().out('knows')").
                getRequestId().get());
        assertEquals("db024fca-ed15-4375-95de-4c6106aef895", GremlinScriptChecker.parse("g.with(\"requestId\", 'db024fca-ed15-4375-95de-4c6106aef895').with(true).V().out('knows')").
                getRequestId().get());
    }

    @Test
    public void shouldIdentifyTimeoutAsStringKeyDoubleQuoted() {
        assertEquals(1000, GremlinScriptChecker.parse("g.with(\"evaluationTimeout\", 1000L).with(true).V().out('knows')").
                getTimeout().get().longValue());
        assertEquals(1000, GremlinScriptChecker.parse("g.with(\"scriptEvaluationTimeout\", 1000L).with(true).V().out('knows')").
                getTimeout().get().longValue());
    }

    @Test
    public void shouldIdentifyRequestIdAsStringKeyDoubleQuoted() {
        assertEquals("db024fca-ed15-4375-95de-4c6106aef895", GremlinScriptChecker.parse("g.with(\"requestId\", \"db024fca-ed15-4375-95de-4c6106aef895\").with(true).V().out('knows')").
                getRequestId().get());
        assertEquals("db024fca-ed15-4375-95de-4c6106aef895", GremlinScriptChecker.parse("g.with(\"requestId\", \"db024fca-ed15-4375-95de-4c6106aef895\").with(true).V().out('knows')").
                getRequestId().get());
    }

    @Test
    public void shouldIdentifyTimeoutAsTokenKey() {
        assertEquals(1000, GremlinScriptChecker.parse("g.with(Tokens.ARGS_EVAL_TIMEOUT, 1000L).with(true).V().out('knows')").
                getTimeout().get().longValue());
        assertEquals(1000, GremlinScriptChecker.parse("g.with(Tokens.ARGS_SCRIPT_EVAL_TIMEOUT, 1000L).with(true).V().out('knows')").
                getTimeout().get().longValue());
    }

    @Test
    public void shouldIdentifyRequestIdAsTokenKey() {
        assertEquals("db024fca-ed15-4375-95de-4c6106aef895", GremlinScriptChecker.parse("g.with(Tokens.REQUEST_ID, \"db024fca-ed15-4375-95de-4c6106aef895\").with(true).V().out('knows')").
                getRequestId().get());
        assertEquals("db024fca-ed15-4375-95de-4c6106aef895", GremlinScriptChecker.parse("g.with(Tokens.REQUEST_ID, \"db024fca-ed15-4375-95de-4c6106aef895\").with(true).V().out('knows')").
                getRequestId().get());
    }

    @Test
    public void shouldIdentifyTimeoutAsTokenKeyWithoutClassName() {
        assertEquals(1000, GremlinScriptChecker.parse("g.with(ARGS_EVAL_TIMEOUT, 1000L).with(true).V().out('knows')").
                getTimeout().get().longValue());
        assertEquals(1000, GremlinScriptChecker.parse("g.with(ARGS_SCRIPT_EVAL_TIMEOUT, 1000L).with(true).V().out('knows')").
                getTimeout().get().longValue());
    }

    @Test
    public void shouldIdentifyRequestIdAsTokenKeyWithoutClassName() {
        assertEquals("db024fca-ed15-4375-95de-4c6106aef895", GremlinScriptChecker.parse("g.with(REQUEST_ID, \"db024fca-ed15-4375-95de-4c6106aef895\").with(true).V().out('knows')").
                getRequestId().get());
        assertEquals("db024fca-ed15-4375-95de-4c6106aef895", GremlinScriptChecker.parse("g.with(REQUEST_ID, 'db024fca-ed15-4375-95de-4c6106aef895').with(true).V().out('knows')").
                getRequestId().get());
    }

    @Test
    public void shouldIdentifyMultipleTimeouts() {
        assertEquals(6000, GremlinScriptChecker.parse("g.with('evaluationTimeout', 1000L).with(true).V().out('knows');" +
                "g.with('evaluationTimeout', 1000L).with(true).V().out('knows');\n" +
                "                                                   //g.with('evaluationTimeout', 1000L).with(true).V().out('knows');\n" +
                "                                                   /* g.with('evaluationTimeout', 1000L).with(true).V().out('knows');*/\n" +
                "                                                   /* \n" +
                "g.with('evaluationTimeout', 1000L).with(true).V().out('knows'); \n" +
                "*/ \n" +
                "                                                   g.with('evaluationTimeout', 1000L).with(true).V().out('knows');\n" +
                "                                                   g.with(Tokens.ARGS_SCRIPT_EVAL_TIMEOUT, 1000L).with(true).V().out('knows');\n" +
                "                                                   g.with(ARGS_EVAL_TIMEOUT, 1000L).with(true).V().out('knows');\n" +
                "                                                   g.with('scriptEvaluationTimeout', 1000L).with(true).V().out('knows');").
                getTimeout().get().longValue());
    }

    @Test
    public void shouldIdentifyMultipleRequestIds() {
        assertEquals("test9", GremlinScriptChecker.parse("g.with('requestId', 'test1').with(true).V().out('knows');" +
                        "g.with('requestId', 'test2').with(true).V().out('knows');\n" +
                        "                                                   //g.with('requestId', 'test3').with(true).V().out('knows');\n" +
                        "                                                   /* g.with('requestId', 'test4').with(true).V().out('knows');*/\n" +
                        "                                                   /* \n" +
                        "g.with('requestId', 'test5').with(true).V().out('knows'); \n" +
                        "*/ \n" +
                        "                                                   g.with('requestId', 'test6').with(true).V().out('knows');\n" +
                        "                                                   g.with(Tokens.REQUEST_ID, 'test7').with(true).V().out('knows');\n" +
                        "                                                   g.with(REQUEST_ID, 'test8').with(true).V().out('knows');\n" +
                        "                                                   g.with('requestId', 'test9').with(true).V().out('knows');").
                getRequestId().get());
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
                "g.with('evaluationTimeout', 1000).with(true).with(REQUEST_ID, \"db024fca-ed15-4375-95de-4c6106aef895\").with(\"materializeProperties\", 'all').V().out('knows')");
        assertEquals(1000, r.getTimeout().get().longValue());
        assertEquals("db024fca-ed15-4375-95de-4c6106aef895", r.getRequestId().get());
        assertEquals("all", r.getMaterializeProperties().get());
    }

    @Test
    public void shouldParseLong() {
        assertEquals(1000, GremlinScriptChecker.parse("g.with('evaluationTimeout', 1000L).addV().property(id, 'blue').as('b').\n" +
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
}
