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
package org.apache.tinkerpop.gremlin.language.grammar;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.apache.commons.lang3.exception.ExceptionUtils;

import static org.junit.Assert.fail;

public class AbstractGrammarTest {

    /**
     * Allows control of the type of parsing to do for a particular test line.
     */
    public enum ParserRule {
        QUERY_LIST
    }

    protected void parse(final String query, final boolean expectFailures) {
        parse(query, ParserRule.QUERY_LIST, expectFailures);
    }

    protected void parse(final String query, final ParserRule rule) {
        parse(query, rule, false);
    }

    protected void parse(final String query, final ParserRule rule, final boolean expectFailures) {

        try {
            final CharStream in = CharStreams.fromString(query);
            final GremlinLexer lexer = new GremlinLexer(in);
            lexer.removeErrorListeners();
            lexer.addErrorListener(new GremlinErrorListener());

            final CommonTokenStream tokens = new CommonTokenStream(lexer);

            // Setup error handler on parser
            final GremlinParser parser = new GremlinParser(tokens);
            parser.setErrorHandler(new BailErrorStrategy());
            parser.getInterpreter().setPredictionMode(PredictionMode.SLL);

            // Visit based on the specified rule
            try {
                final GremlinBaseVisitor<?> visitor = new GremlinBaseVisitor<>();

                switch (rule) {
                    case QUERY_LIST:
                        visitor.visit(parser.queryList());
                        break;
                }
            } catch (Exception ex) {
                // Retry parsing with LL prediction mode
                tokens.seek(0);
                lexer.reset();
                parser.reset();
                parser.getInterpreter().setPredictionMode(PredictionMode.LL);

                final GremlinBaseVisitor<?> visitor = new GremlinBaseVisitor<>();

                switch (rule) {
                    case QUERY_LIST:
                        visitor.visit(parser.queryList());
                        break;
                }
            }

            if (expectFailures) {
                fail("Query parsing should have failed for " + query);
            }

        } catch (Exception e) {
            if (!expectFailures) {
                final Throwable t = ExceptionUtils.getRootCause(e);
                if (t instanceof LexerNoViableAltException) {
                    fail(String.format("Error parsing: %s - [%s] - failed at index: %s",
                            query, t.toString(), ((LexerNoViableAltException) t).getStartIndex()));
                } else if (t instanceof NoViableAltException) {
                    fail(String.format("Error parsing: %s - [%s]", query, ((NoViableAltException) t).getStartToken().toString()));
                } else {
                    fail(String.format("Error parsing: %s - [%s]", query, t.getMessage()));
                }
            }
        }
    }
}

