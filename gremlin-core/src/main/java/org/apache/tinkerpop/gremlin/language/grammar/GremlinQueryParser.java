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
package org.apache.tinkerpop.gremlin.language.grammar;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import javax.lang.model.SourceVersion;

/**
 * Parses Gremlin strings to an {@code Object}, typically to a {@link Traversal}.
 */
public class GremlinQueryParser {
    private static final Logger log = LoggerFactory.getLogger(GremlinQueryParser.class);
    private static final GremlinErrorListener errorListener = new GremlinErrorListener();

    /**
     * Parse Gremlin string using a default {@link GremlinAntlrToJava} object.
     */
    public static Object parse(final String query) {
        return parse(query, new GremlinAntlrToJava());
    }

    /**
     * Parse Gremlin string using a specified {@link GremlinAntlrToJava} object.
     */
    public static Object parse(final String query, final GremlinVisitor<Object> visitor)  {
        final GremlinLexer lexer = createLexer(query);
        final CommonTokenStream tokens = new CommonTokenStream(lexer);

        // Setup error handler on parser
        final GremlinParser parser = createParser(tokens);
        // SLL prediction mode is faster than the LL prediction mode when parsing the grammar,
        // but it does not cover parsing all types of input.  We use the SLL by default, and fallback
        // to LL mode if fails to parse the query.
        parser.getInterpreter().setPredictionMode(PredictionMode.SLL);

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
            throw new GremlinParserException("Failed to interpret Gremlin query: " + ex.getMessage(), ex);
        }
    }

    /**
     * Parses a gremlin-lang map literal string into a {@code Map<String, Object>} for use as parameters.
     * <p>
     * Uses {@link ParameterMapVisitor} to prevent traversal injection and validates that all keys are strings
     * and no values contain traversals.
     *
     * @param parameterMapString the gremlin-lang map literal string (e.g. {@code [x:1,y:"marko"]}) or {@code null}/empty
     * @return the parsed and validated parameter map
     * @throws GremlinParserException if parsing fails or validation detects invalid content
     */
    public static Map<String, Object> parseParameters(final String parameterMapString) {
        if (parameterMapString == null || parameterMapString.isEmpty()) {
            return Map.of();
        }

        final GremlinParser parser = createParser(parameterMapString);
        final GremlinParser.GenericMapLiteralContext mapCtx = parser.genericMapLiteral();

        final ParameterMapVisitor visitor = new ParameterMapVisitor(new GremlinAntlrToJava());
        final Map<Object, Object> rawMap = (Map<Object, Object>) visitor.visitGenericMapLiteral(mapCtx);

        if (rawMap == null) {
            return Map.of();
        }

        for (final Map.Entry<?, ?> entry : rawMap.entrySet()) {
            if (!(entry.getKey() instanceof String)) {
                throw new GremlinParserException(
                        String.format("Parameter map keys must be String, found: %s",
                                entry.getKey() == null ? "null" : entry.getKey().getClass().getSimpleName()));
            }
            final String key = (String) entry.getKey();
            if (!SourceVersion.isIdentifier(key)) {
                throw new GremlinParserException(
                        String.format("Parameter map key must be a valid identifier: %s", key));
            }
            validateParameterValue(entry.getValue());
        }

        return (Map<String, Object>) (Map<?, ?>) rawMap;
    }

    /**
     * Recursively validates that a parameter value does not contain a {@link Traversal}. Nested validation is needed
     * because steps like mergeV iterate map values, so a Traversal hiding inside a nested map or collection would still
     * be dangerous.
     */
    private static void validateParameterValue(final Object value) {
        if (value instanceof Traversal) {
            throw new GremlinParserException("Traversals are not allowed as parameter values");
        }
        if (value instanceof Map) {
            for (final Map.Entry<?, ?> e : ((Map<?, ?>) value).entrySet()) {
                validateParameterValue(e.getKey());
                validateParameterValue(e.getValue());
            }
        }
        if (value instanceof Collection) {
            for (final Object v : (Collection<?>) value) {
                validateParameterValue(v);
            }
        }
    }

    /**
     * Creates a {@link GremlinParser} from the given input string.
     */
    private static GremlinParser createParser(final String input) {
        return createParser(new CommonTokenStream(createLexer(input)));
    }

    /**
     * Creates a {@link GremlinParser} from the given {@link GremlinLexer}.
     */
    private static GremlinParser createParser(final CommonTokenStream tokens) {
        final GremlinParser parser = new GremlinParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(errorListener);
        return parser;
    }

    /**
     * Creates a {@link GremlinLexer} from the given input string with error listeners configured.
     */
    private static GremlinLexer createLexer(final String input) {
        final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(input));
        lexer.removeErrorListeners();
        lexer.addErrorListener(errorListener);
        return lexer;
    }
}
