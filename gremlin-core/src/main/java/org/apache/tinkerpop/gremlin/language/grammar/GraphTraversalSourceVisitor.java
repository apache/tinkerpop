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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 * This class implements the {@link GraphTraversalSource} producing methods of Gremlin grammar.
 */
public class GraphTraversalSourceVisitor extends DefaultGremlinBaseVisitor<GraphTraversalSource> {
    public static final String TRAVERSAL_ROOT = "g";
    private final Graph graph;
    private final GremlinAntlrToJava antlr;
    private final String traversalSourceName;
    private final GraphTraversalSource g;

    /**
     * Constructs the visitor and if the {@link GremlinAntlrToJava} has a {@link GraphTraversalSource} assigned to it,
     * the visitor will prefer that "g" rather than creating a new one from the associated {@link Graph} instance.
     */
    public GraphTraversalSourceVisitor(final GremlinAntlrToJava antlr) {
        this(TRAVERSAL_ROOT, antlr);
    }

    /**
     * Same as {@link #GraphTraversalSourceVisitor(GremlinAntlrToJava)} but allows the traversal source name to be
     * configured to something other than "g".
     */
    public GraphTraversalSourceVisitor(final String traversalSourceName, final GremlinAntlrToJava antlr) {
        this.graph = antlr.graph;
        this.antlr = antlr;
        this.g = antlr.g;
        this.traversalSourceName = traversalSourceName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversalSource visitTraversalSource(final GremlinParser.TraversalSourceContext ctx) {
        if (ctx.getChildCount() == 1) {
            // handle source method only
            return null == g ? graph.traversal() : g;
        } else {
            final int childIndexOfSelfMethod = 2;
            GraphTraversalSource source;
            if (ctx.getChild(0).getText().equals(traversalSourceName)) {
                // handle single traversal source
                source = null == g ? graph.traversal() : g;
            } else {
                // handle chained self method
                final int childIndexOfTraversalSource = 0;
                source = visitTraversalSource(
                        (GremlinParser.TraversalSourceContext) ctx.getChild(childIndexOfTraversalSource));
            }
            return new TraversalSourceSelfMethodVisitor(source, antlr).visitTraversalSourceSelfMethod(
                    (GremlinParser.TraversalSourceSelfMethodContext) (ctx.getChild(childIndexOfSelfMethod)));
        }
    }
}
