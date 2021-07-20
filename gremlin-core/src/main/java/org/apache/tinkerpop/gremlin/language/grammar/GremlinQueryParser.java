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

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GremlinQueryParser {
    private static final Logger log = LoggerFactory.getLogger(GremlinQueryParser.class);
    private static final GremlinErrorListener errorListener = new GremlinErrorListener();

    public static Object parse(final String query) throws Exception {
        return parse(query, new GremlinAntlrToJava());
    }

    public static Object parse(final String query, final GremlinVisitor<Object> visitor)  throws Exception  {
        final CharStream in = CharStreams.fromString(query);
        final GremlinLexer lexer = new GremlinLexer(in);
        lexer.removeErrorListeners();
        lexer.addErrorListener(errorListener);

        final CommonTokenStream tokens = new CommonTokenStream(lexer);

        // Setup error handler on parser
        final GremlinParser parser = new GremlinParser(tokens);
        // SLL prediction mode is faster than the LL prediction mode when parsing the grammar,
        // but it does not cover parsing all types of input.  We use the SLL by default, and fallback
        // to LL mode if fails to parse the query.
        parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
        parser.removeErrorListeners();
        parser.addErrorListener(errorListener);

        GremlinParser.QueryListContext queryContext;
        try {
            queryContext = parser.queryList();
        } catch (Exception ex) {
            // Retry parsing the query again with using LL prediction mode.  LL parsing mode is more powerful
            // so retrying the parsing would help parsing the rare edge cases.
            try {
                tokens.seek(0); // rewind input stream
                lexer.reset();
                parser.reset();
                parser.getInterpreter().setPredictionMode(PredictionMode.LL);
                log.debug("Query parsed with using LL prediction mode: {}", query);
                queryContext = parser.queryList();
            } catch (Exception e) {
                log.debug("Query parsing failed in retry with exception" + e);
                throw new GremlinParserException("Failed to interpret Gremlin query: " + e.getMessage());
            }        
        }

        try {
            return visitor.visit(queryContext);
        } catch (ClassCastException ex) {
            // Special case that can be interpreted as a (semantic) parse error.
            // Do not include ex as the cause - org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor.eval()
            // in Tinkerpop v 3.3.0, strips root causes off their outer exception objects.
            // We just log ex here for debugging purposes.
            log.debug("Converting a java.lang.ClassCastException to GremlinParserException," +
                    " assuming that it indicates a semantic parse error.", ex);
            throw new GremlinParserException("Failed to interpret Gremlin query: " + ex.getMessage());
        }
    }
}
