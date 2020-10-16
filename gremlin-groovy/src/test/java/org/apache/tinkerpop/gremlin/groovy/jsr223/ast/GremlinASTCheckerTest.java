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
package org.apache.tinkerpop.gremlin.groovy.jsr223.ast;

import org.codehaus.groovy.control.MultipleCompilationErrorsException;
import org.junit.Test;

import java.util.Optional;

import static org.apache.tinkerpop.gremlin.groovy.jsr223.ast.GremlinASTChecker.EMPTY_RESULT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class GremlinASTCheckerTest {

    @Test
    public void shouldNotFindTimeout() {
        assertEquals(Optional.empty(), GremlinASTChecker.parse("g.with(true).V().out('knows')").getTimeout());
    }

    @Test
    public void shouldReturnEmpty() {
        assertSame(EMPTY_RESULT, GremlinASTChecker.parse(""));
    }

    @Test(expected = MultipleCompilationErrorsException.class)
    public void shouldNotParse() {
        GremlinASTChecker.parse("g.with('evaluationTimeout', 1000L).with(true).V().out('knows'))");
    }

    @Test
    public void shouldNotFindTimeoutCozWeCommentedItOut() {
        assertEquals(Optional.empty(), GremlinASTChecker.parse("g.\n" +
                "                                                  // with('evaluationTimeout', 1000L).\n" +
                "                                                  with(true).V().out('knows')").getTimeout());
    }

    @Test
    public void shouldIdentifyTimeoutAsStringKey() {
        assertEquals(1000, GremlinASTChecker.parse("g.with('evaluationTimeout', 1000L).with(true).V().out('knows')").
                getTimeout().get().longValue());
    }

    @Test
    public void shouldIdentifyTimeoutAsTokenKey() {
        assertEquals(1000, GremlinASTChecker.parse("g.with(Tokens.ARGS_EVAL_TIMEOUT, 1000L).with(true).V().out('knows')").
                getTimeout().get().longValue());
    }

    @Test
    public void shouldIdentifyTimeoutAsTokenKeyWithoutClassName() {
        assertEquals(1000, GremlinASTChecker.parse("g.with(ARGS_EVAL_TIMEOUT, 1000L).with(true).V().out('knows')").
                getTimeout().get().longValue());
    }

    @Test
    public void shouldIdentifyMultipleTimeouts() {
        assertEquals(6000, GremlinASTChecker.parse("g.with('evaluationTimeout', 1000L).with(true).V().out('knows');" +
                "g.with('evaluationTimeout', 1000L).with(true).V().out('knows')\n" +
                "                                                   //g.with('evaluationTimeout', 1000L).with(true).V().out('knows')\n" +
                "                                                   g.with('evaluationTimeout', 1000L).with(true).V().out('knows')\n" +
                "                                                   g.with(Tokens.ARGS_SCRIPT_EVAL_TIMEOUT, 1000L).with(true).V().out('knows')\n" +
                "                                                   g.with(ARGS_EVAL_TIMEOUT, 1000L).with(true).V().out('knows')\n" +
                "                                                   g.with('scriptEvaluationTimeout', 1000L).with(true).V().out('knows')").
                getTimeout().get().longValue());
    }
}
