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
package org.apache.tinkerpop.gremlin.gql;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.gql.GQLLexer;
import org.apache.tinkerpop.gremlin.gql.GQLParser;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The logical graph structure produced by parsing a GQL MATCH clause. A {@code QueryGraph}
 * holds the complete set of {@link QueryVertex} and {@link QueryEdge} objects that represent
 * the pattern to match, preserving the variable-identity relationships across multiple
 * comma-separated path patterns.
 *
 * <p>Variable identity is enforced: if the same variable name appears in multiple patterns,
 * the corresponding {@link QueryVertex} instances are shared (reference-equal) in the
 * resulting graph.
 *
 * <p>Use {@link #parse(String)} to construct a {@code QueryGraph} from a GQL MATCH string.
 */
public final class QueryGraph {

    private final List<QueryVertex> nodes;
    private final List<QueryEdge> edges;

    private QueryGraph(final List<QueryVertex> nodes, final List<QueryEdge> edges) {
        this.nodes = Collections.unmodifiableList(nodes);
        this.edges = Collections.unmodifiableList(edges);
    }

    /**
     * Returns the ordered list of all nodes in this pattern graph.
     */
    public List<QueryVertex> getNodes() {
        return nodes;
    }

    /**
     * Returns the ordered list of all edges in this pattern graph.
     */
    public List<QueryEdge> getEdges() {
        return edges;
    }

    /**
     * Parses a GQL MATCH string and returns the corresponding {@code QueryGraph}.
     *
     * <p>The input must begin with the {@code MATCH} keyword followed by one or more
     * comma-separated path patterns. Example:
     * <pre>
     *   MATCH (a:Person)-[:KNOWS]-&gt;(b:Person)
     *   MATCH (a:Person)-[:KNOWS]-&gt;(b:Person), (b)-[:WORKS_AT]-&gt;(c:Company)
     * </pre>
     *
     * @param gqlMatchString a GQL MATCH expression
     * @return the constructed {@code QueryGraph}
     * @throws IllegalArgumentException if the string cannot be parsed
     */
    public static QueryGraph parse(final String gqlMatchString) {
        final GQLLexer lexer = new GQLLexer(CharStreams.fromString(gqlMatchString));
        lexer.removeErrorListeners();
        final CommonTokenStream tokens = new CommonTokenStream(lexer);
        final GQLParser parser = new GQLParser(tokens);
        parser.removeErrorListeners();
        parser.setErrorHandler(new BailErrorStrategy());
        parser.getInterpreter().setPredictionMode(PredictionMode.SLL);

        try {
            return fromParseTree(parser.matchClause());
        } catch (final Exception ex) {
            // Retry with full LL prediction on SLL failure
            tokens.seek(0);
            lexer.reset();
            parser.reset();
            parser.getInterpreter().setPredictionMode(PredictionMode.LL);
            try {
                return fromParseTree(parser.matchClause());
            } catch (final Exception retryEx) {
                throw new IllegalArgumentException("Failed to parse GQL MATCH expression: " + gqlMatchString, retryEx);
            }
        }
    }

    /**
     * Builds a {@code QueryGraph} by walking the given ANTLR parse tree rooted at a
     * {@code matchClause} context. Shared variable names across patterns are resolved
     * to the same {@link QueryVertex} instance.
     */
    static QueryGraph fromParseTree(final GQLParser.MatchClauseContext ctx) {
        // nodes keyed by variable name for deduplication; anonymous nodes are always distinct
        final Map<String, QueryVertex> nodesByVar = new LinkedHashMap<>();
        final List<QueryVertex> nodes = new ArrayList<>();
        final List<QueryEdge> edges = new ArrayList<>();
        // Track edge variable names to detect node/edge variable name conflicts.
        final java.util.Set<String> edgeVarNames = new java.util.HashSet<>();

        for (final GQLParser.PathPatternContext patternCtx : ctx.graphPattern().pathPattern()) {
            final List<GQLParser.NodePatternContext> nodeCtxs = patternCtx.nodePattern();
            final List<GQLParser.EdgePatternContext> edgeCtxs = patternCtx.edgePattern();

            // Build the first node in this chain
            QueryVertex current = resolveNode(nodeCtxs.get(0), nodesByVar, nodes);

            // Walk the (edgePattern nodePattern)* chain
            for (int i = 0; i < edgeCtxs.size(); i++) {
                final GQLParser.EdgePatternContext edgeCtx = edgeCtxs.get(i);
                final QueryVertex next = resolveNode(nodeCtxs.get(i + 1), nodesByVar, nodes);

                final String edgeVar = extractEdgeVariable(edgeCtx);
                if (edgeVar != null) {
                    if (nodesByVar.containsKey(edgeVar))
                        throw new IllegalArgumentException(
                                "Variable '" + edgeVar + "' is used as both a node variable and an edge variable");
                    edgeVarNames.add(edgeVar);
                }
                final String edgeLabel = extractEdgeLabel(edgeCtx);
                final Direction dir = extractDirection(edgeCtx);
                final List<PropertyPredicate> edgePredicates = extractEdgePredicates(edgeCtx);

                edges.add(new QueryEdge(edgeVar, edgeLabel, dir, current, next, edgePredicates));
                current = next;
            }
        }

        // Validate that no node variable was introduced after an edge variable with the same name.
        for (final String nodeVar : nodesByVar.keySet()) {
            if (edgeVarNames.contains(nodeVar))
                throw new IllegalArgumentException(
                        "Variable '" + nodeVar + "' is used as both a node variable and an edge variable");
        }

        return new QueryGraph(nodes, edges);
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private static QueryVertex resolveNode(final GQLParser.NodePatternContext ctx,
                                         final Map<String, QueryVertex> nodesByVar,
                                         final List<QueryVertex> nodes) {
        final GQLParser.ElementPatternFillerContext contents = ctx.elementPatternFiller();
        final String var;
        final String label;

        if (contents.elementVariable() != null) {
            var = contents.elementVariable().IDENTIFIER().getText();
            label = contents.labelSpec() != null ? contents.labelSpec().labelName().IDENTIFIER().getText() : null;
        } else if (contents.labelSpec() != null) {
            // COLON label only
            var = null;
            label = contents.labelSpec().labelName().IDENTIFIER().getText();
        } else {
            // Anonymous node: ()
            var = null;
            label = null;
        }

        final List<PropertyPredicate> predicates = extractPredicates(contents);

        if (var != null) {
            // Reuse existing node with this variable name. A later occurrence may refine
            // an unlabeled variable with a label, or add predicates — both are merged.
            // Conflicting labels (both non-null and different) are rejected.
            if (nodesByVar.containsKey(var)) {
                final QueryVertex existing = nodesByVar.get(var);
                if (existing.getLabel() != null && label != null && !label.equals(existing.getLabel())) {
                    throw new IllegalArgumentException(
                            "Variable '" + var + "' is used with conflicting label constraints: '"
                            + existing.getLabel() + "' vs '" + label + "'");
                }
                existing.merge(label, predicates);
                return existing;
            }
            final QueryVertex n = new QueryVertex(var, label, predicates);
            nodesByVar.put(var, n);
            nodes.add(n);
            return n;
        } else {
            // Anonymous node — always a new instance
            final QueryVertex n = new QueryVertex(null, label, predicates);
            nodes.add(n);
            return n;
        }
    }

    private static List<PropertyPredicate> extractPredicates(
            final GQLParser.ElementPatternFillerContext ctx) {
        if (ctx.propertyFilter() == null) return Collections.emptyList();
        return ctx.propertyFilter().propertyPair().stream()
                .map(pair -> {
                    final String key = pair.propertyKey().IDENTIFIER().getText();
                    final GQLParser.PropertyValueContext valCtx = pair.propertyValue();
                    if (valCtx.paramRef() != null) {
                        final String paramName = valCtx.paramRef().IDENTIFIER().getText();
                        return PropertyPredicate.ofParam(key, paramName);
                    }
                    return PropertyPredicate.ofLiteral(key, parseLiteral(valCtx.literal()));
                })
                .collect(Collectors.toList());
    }

    private static Object parseLiteral(final GQLParser.LiteralContext ctx) {
        if (ctx.STRING_LITERAL() != null) {
            final String raw = ctx.STRING_LITERAL().getText();
            // Strip surrounding single or double quotes, then unescape.
            return unescapeString(raw.substring(1, raw.length() - 1));
        }
        if (ctx.FLOAT_LITERAL() != null) {
            final String text = ctx.FLOAT_LITERAL().getText();
            final char last = Character.toLowerCase(text.charAt(text.length() - 1));
            // Strip the type suffix (if any) before parsing.
            final String num = Character.isLetter(last) ? text.substring(0, text.length() - 1) : text;
            if (last == 'm') return new BigDecimal(num);
            if (last == 'f') return Float.parseFloat(num);
            return Double.parseDouble(num);   // 'd' suffix or no suffix → Double
        }
        if (ctx.INTEGER_LITERAL() != null) {
            final String text = ctx.INTEGER_LITERAL().getText();
            final char last = Character.toLowerCase(text.charAt(text.length() - 1));
            final String num = Character.isLetter(last) ? text.substring(0, text.length() - 1) : text;
            switch (last) {
                case 'b': return Byte.parseByte(num);
                case 's': return Short.parseShort(num);
                case 'i': return Integer.parseInt(num);
                case 'l': return Long.parseLong(num);
                case 'n':
                    // BigInteger does not accept a leading '+'.
                    final String bigNum = num.startsWith("+") ? num.substring(1) : num;
                    return new BigInteger(bigNum);
            }
            // No type suffix: match Gremlin's default — smallest fitting type.
            try { return Integer.parseInt(text); }
            catch (final NumberFormatException e1) {
                try { return Long.parseLong(text); }
                catch (final NumberFormatException e2) {
                    final String s = text.startsWith("+") ? text.substring(1) : text;
                    return new BigInteger(s);
                }
            }
        }
        if (ctx.K_TRUE()  != null) return Boolean.TRUE;
        if (ctx.K_FALSE() != null) return Boolean.FALSE;
        if (ctx.K_NULL()  != null) return null;
        if (ctx.K_NAN()   != null) return Double.NaN;
        if (ctx.K_INFINITY() != null) return Double.POSITIVE_INFINITY;
        if (ctx.SIGNED_INFINITY() != null) {
            return ctx.SIGNED_INFINITY().getText().charAt(0) == '-'
                   ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
        }
        throw new IllegalStateException("Unrecognized literal: " + ctx.getText());
    }

    /**
     * Applies Java-style escape sequences to a string that has already had its
     * surrounding quotes removed. Handles: \b \t \n \f \r \" \' \\,
     * octal escapes, and four-hex-digit unicode escapes.
     */
    private static String unescapeString(final String s) {
        if (s.indexOf('\\') < 0) return s; // fast path: nothing to unescape
        final StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            final char c = s.charAt(i);
            if (c != '\\' || i + 1 >= s.length()) {
                sb.append(c);
                continue;
            }
            final char next = s.charAt(++i);
            switch (next) {
                case 'b':  sb.append('\b'); break;
                case 't':  sb.append('\t'); break;
                case 'n':  sb.append('\n'); break;
                case 'f':  sb.append('\f'); break;
                case 'r':  sb.append('\r'); break;
                case '"':  sb.append('"');  break;
                case '\'': sb.append('\''); break;
                case '\\': sb.append('\\'); break;
                case 'u':
                    if (i + 4 < s.length()) {
                        sb.append((char) Integer.parseInt(s.substring(i + 1, i + 5), 16));
                        i += 4;
                    } else {
                        sb.append('\\').append(next);
                    }
                    break;
                default:
                    if (next >= '0' && next <= '7') {
                        int val = next - '0';
                        if (i + 1 < s.length() && s.charAt(i + 1) >= '0' && s.charAt(i + 1) <= '7') {
                            val = val * 8 + (s.charAt(++i) - '0');
                            if (next <= '3' && i + 1 < s.length()
                                    && s.charAt(i + 1) >= '0' && s.charAt(i + 1) <= '7') {
                                val = val * 8 + (s.charAt(++i) - '0');
                            }
                        }
                        sb.append((char) val);
                    } else {
                        sb.append('\\').append(next);
                    }
                    break;
            }
        }
        return sb.toString();
    }

    private static List<PropertyPredicate> extractEdgePredicates(final GQLParser.EdgePatternContext ctx) {
        final GQLParser.ElementPatternFillerContext filler = getEdgeFiller(ctx);
        if (filler == null) return Collections.emptyList();
        return extractPredicates(filler);
    }

    private static String extractEdgeVariable(final GQLParser.EdgePatternContext ctx) {
        final GQLParser.ElementPatternFillerContext filler = getEdgeFiller(ctx);
        if (filler == null || filler.elementVariable() == null) return null;
        return filler.elementVariable().IDENTIFIER().getText();
    }

    private static String extractEdgeLabel(final GQLParser.EdgePatternContext ctx) {
        final GQLParser.ElementPatternFillerContext filler = getEdgeFiller(ctx);
        if (filler == null) return null;
        final GQLParser.LabelSpecContext labelSpec = filler.labelSpec();
        return labelSpec != null ? labelSpec.labelName().IDENTIFIER().getText() : null;
    }

    private static Direction extractDirection(final GQLParser.EdgePatternContext ctx) {
        if (ctx.reverseDirectedEdge() != null) return Direction.IN;
        if (ctx.directedEdge() != null) return Direction.OUT;
        return Direction.BOTH;
    }

    private static GQLParser.ElementPatternFillerContext getEdgeFiller(final GQLParser.EdgePatternContext ctx) {
        if (ctx.reverseDirectedEdge() != null) return ctx.reverseDirectedEdge().elementPatternFiller();
        if (ctx.directedEdge() != null) return ctx.directedEdge().elementPatternFiller();
        if (ctx.undirectedEdge() != null) return ctx.undirectedEdge().elementPatternFiller();
        return null;
    }

    @Override
    public String toString() {
        return "QueryGraph{nodes=" + nodes + ", edges=" + edges + "}";
    }
}
