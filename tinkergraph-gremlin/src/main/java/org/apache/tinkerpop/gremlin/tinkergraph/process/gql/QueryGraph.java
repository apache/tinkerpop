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
package org.apache.tinkerpop.gremlin.tinkergraph.process.gql;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.tinkergraph.gql.GQLLexer;
import org.apache.tinkerpop.gremlin.tinkergraph.gql.GQLParser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The logical graph structure produced by parsing a GQL MATCH clause. A {@code QueryGraph}
 * holds the complete set of {@link QueryNode} and {@link QueryEdge} objects that represent
 * the pattern to match, preserving the variable-identity relationships across multiple
 * comma-separated path patterns.
 *
 * <p>Variable identity is enforced: if the same variable name appears in multiple patterns,
 * the corresponding {@link QueryNode} instances are shared (reference-equal) in the
 * resulting graph.
 *
 * <p>Use {@link #parse(String)} to construct a {@code QueryGraph} from a GQL MATCH string.
 */
public final class QueryGraph {

    private final List<QueryNode> nodes;
    private final List<QueryEdge> edges;

    private QueryGraph(final List<QueryNode> nodes, final List<QueryEdge> edges) {
        this.nodes = Collections.unmodifiableList(nodes);
        this.edges = Collections.unmodifiableList(edges);
    }

    /**
     * Returns the ordered list of all nodes in this pattern graph.
     */
    public List<QueryNode> getNodes() {
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
     * to the same {@link QueryNode} instance.
     */
    static QueryGraph fromParseTree(final GQLParser.MatchClauseContext ctx) {
        // nodes keyed by variable name for deduplication; anonymous nodes are always distinct
        final Map<String, QueryNode> nodesByVar = new LinkedHashMap<>();
        final List<QueryNode> nodes = new ArrayList<>();
        final List<QueryEdge> edges = new ArrayList<>();

        for (final GQLParser.PathPatternContext patternCtx : ctx.graphPattern().pathPattern()) {
            final List<GQLParser.NodePatternContext> nodeCtxs = patternCtx.nodePattern();
            final List<GQLParser.EdgePatternContext> edgeCtxs = patternCtx.edgePattern();

            // Build the first node in this chain
            QueryNode current = resolveNode(nodeCtxs.get(0), nodesByVar, nodes);

            // Walk the (edgePattern nodePattern)* chain
            for (int i = 0; i < edgeCtxs.size(); i++) {
                final GQLParser.EdgePatternContext edgeCtx = edgeCtxs.get(i);
                final QueryNode next = resolveNode(nodeCtxs.get(i + 1), nodesByVar, nodes);

                final String edgeVar = extractEdgeVariable(edgeCtx);
                final String edgeLabel = extractEdgeLabel(edgeCtx);
                final Direction dir = extractDirection(edgeCtx);

                edges.add(new QueryEdge(edgeVar, edgeLabel, dir, current, next));
                current = next;
            }
        }

        return new QueryGraph(nodes, edges);
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private static QueryNode resolveNode(final GQLParser.NodePatternContext ctx,
                                         final Map<String, QueryNode> nodesByVar,
                                         final List<QueryNode> nodes) {
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

        if (var != null) {
            // Reuse existing node with this variable name. If the same variable appears
            // with different label constraints (e.g. MATCH (n:Person)-[:K]->(n:Animal)),
            // reject it — a variable must refer to a single consistent node type.
            if (nodesByVar.containsKey(var)) {
                final QueryNode existing = nodesByVar.get(var);
                if (label != null && !label.equals(existing.getLabel())) {
                    throw new IllegalArgumentException(
                            "Variable '" + var + "' is used with conflicting label constraints: '"
                            + existing.getLabel() + "' vs '" + label + "'");
                }
                return existing;
            }
            final QueryNode n = new QueryNode(var, label);
            nodesByVar.put(var, n);
            nodes.add(n);
            return n;
        } else {
            // Anonymous node — always a new instance
            final QueryNode n = new QueryNode(null, label);
            nodes.add(n);
            return n;
        }
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
