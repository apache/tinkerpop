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

    protected void parse(final String query) {
        parse(query, false);
    }

    protected void parse(final String query, final boolean expectFailures) {
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

            // Visit top level query
            try {
                final GremlinVisitor<GremlinParser.QueryListContext> visitor = new GremlinBaseVisitor<>();
                visitor.visit(parser.queryList());
            } catch (Exception ex) {
                // Retry parsing the query again with using LL prediction mode.  LL parsing mode is more powerful
                // so retrying the parsing would help parsing the rare edge cases.
                tokens.seek(0); // rewind input stream
                lexer.reset();
                parser.reset();
                parser.getInterpreter().setPredictionMode(PredictionMode.LL);

                final GremlinVisitor<GremlinParser.QueryListContext> visitor = new GremlinBaseVisitor<>();
                visitor.visit(parser.queryList());
            }

            // If we are expecting failures, put assert check here
            if (expectFailures) {
                fail("Query parsing should have failed for " + query);
            }

        } catch(Exception e) {
            if (!expectFailures) {
                final Throwable t = ExceptionUtils.getRootCause(e);
                if (t instanceof LexerNoViableAltException) {
                    fail(String.format("Error parsing query: %s - [%s] - failed at index: %s",
                            query, t.toString(), ((LexerNoViableAltException) t).getStartIndex()));
                } else if (t instanceof NoViableAltException) {
                    fail(String.format("Error parsing query: %s - [%s]", query, ((NoViableAltException) t).getStartToken().toString()));
                } else {
                    fail(String.format("Error parsing query: %s - [%s]", query, t.getMessage()));
                }
            }
        }
    }

}

