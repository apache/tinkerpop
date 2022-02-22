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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

/**
 * This visitor handles the cases when a new traversal is getting started. It could either be a nested traversal
 * or a root traversal, but in either case should be anonymous.
 */
public class TraversalRootVisitor<G extends Traversal> extends DefaultGremlinBaseVisitor<Traversal> {
    private Traversal traversal;
    protected final GremlinAntlrToJava antlr;

    /**
     * Constructor to produce an anonymous traversal.
     */
    public TraversalRootVisitor(final GremlinAntlrToJava antlr) {
        this(antlr, null);
    }

    /**
     * Constructor to build on an existing traversal.
     */
    public TraversalRootVisitor(final Traversal traversal) {
        this(null, traversal);
    }

    public TraversalRootVisitor(final GremlinAntlrToJava antlr, final Traversal traversal) {
        this.traversal = traversal;
        this.antlr = antlr;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Traversal visitNestedTraversal(final GremlinParser.NestedTraversalContext ctx) {
        if (ctx.getChild(0) instanceof GremlinParser.RootTraversalContext) {
            return visitChildren(ctx);
        } else if (ctx.getChild(2) instanceof GremlinParser.ChainedParentOfGraphTraversalContext) {
            return new TraversalRootVisitor<Traversal>(antlr.createAnonymous.get()).
                    visitChainedParentOfGraphTraversal(ctx.chainedTraversal().chainedParentOfGraphTraversal());
        } else {
            return new TraversalMethodVisitor(antlr, antlr.createAnonymous.get()).visitChainedTraversal(ctx.chainedTraversal());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Traversal visitRootTraversal(final GremlinParser.RootTraversalContext ctx) {
        // create traversal source
        final int childIndexOfTraversalSource = 0;
        final GraphTraversalSource source = antlr.gvisitor.visitTraversalSource(
                (GremlinParser.TraversalSourceContext) ctx.getChild(childIndexOfTraversalSource));
        // call traversal source spawn method
        final int childIndexOfTraversalSourceSpawnMethod = 2;
        final GraphTraversal traversal = new TraversalSourceSpawnMethodVisitor(source, this).visitTraversalSourceSpawnMethod(
                (GremlinParser.TraversalSourceSpawnMethodContext) ctx.getChild(childIndexOfTraversalSourceSpawnMethod));

        if (ctx.getChildCount() == 5) {
            // handle chained traversal
            final int childIndexOfChainedTraversal = 4;

            if (ctx.getChild(childIndexOfChainedTraversal) instanceof GremlinParser.ChainedParentOfGraphTraversalContext) {
                final TraversalRootVisitor traversalRootVisitor = new TraversalRootVisitor(traversal);
                return traversalRootVisitor.visitChainedParentOfGraphTraversal(
                        (GremlinParser.ChainedParentOfGraphTraversalContext) ctx.getChild(childIndexOfChainedTraversal));
            } else {
                final TraversalMethodVisitor traversalMethodVisitor = new TraversalMethodVisitor(antlr, traversal);
                return traversalMethodVisitor.visitChainedTraversal(
                        (GremlinParser.ChainedTraversalContext) ctx.getChild(childIndexOfChainedTraversal));
            }
        } else {
            return traversal;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Traversal visitTraversalSelfMethod(final GremlinParser.TraversalSelfMethodContext ctx) {
        return visitChildren(ctx);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Traversal visitTraversalSelfMethod_none(final GremlinParser.TraversalSelfMethod_noneContext ctx) {
        this.traversal = traversal.none();
        return this.traversal;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Traversal visitChainedParentOfGraphTraversal(final GremlinParser.ChainedParentOfGraphTraversalContext ctx) {
        if (ctx.getChildCount() == 1) {
            return visitChildren(ctx);
        } else {
            visit(ctx.getChild(0));
            return visit(ctx.getChild(2));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Traversal visitChainedTraversal(final GremlinParser.ChainedTraversalContext ctx) {
        if (ctx.getChildCount() == 1) {
            return visitChildren(ctx);
        } else {
            visit(ctx.getChild(0));
            return visit(ctx.getChild(2));
        }
    }
}
